// A simple script to extract RDS backup data from AWS Athena and insert it into a Postgres database.

import {fromEnv} from "@aws-sdk/credential-providers";
import {
    AthenaClient,
    GetQueryExecutionCommand,
    GetQueryResultsCommand,
    GetTableMetadataCommand,
    StartQueryExecutionCommand
} from "@aws-sdk/client-athena";
import {config} from 'dotenv';
import pg from "pg";
import fs from 'fs';
import util from 'util';

const logFile = fs.createWriteStream("/Users/xyz/pet-projects/pg_insertion2.log", { flags: 'a' });
const logStdout = process.stdout;

console.log = function () {
    logFile.write(util.format.apply(null, arguments) + '\n');
    logStdout.write(util.format.apply(null, arguments) + '\n');
};

console.error = console.log;

const {Pool: PgPool} = pg;

config();
console.log("Start time: ", new Date().toISOString());
console.log(`Process ID: ${process.pid}`);

process.env.AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID_L
process.env.AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY_L
const AWS_REGION = process.env.AWS_REGION;
const DB_NAME = process.env.DB_NAME;
const CATALOG_NAME = process.env.CATALOG_NAME;
const ATHENA_OUTPUT_S3_BUCKET = process.env.ATHENA_OUTPUT_S3_BUCKET;
const DB_SCHEMA = process.env.PG_SCHEMA;

const pgPool = new PgPool({
    host: process.env.PG_HOST,
    port: process.env.PG_PORT,
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    database: process.env.PG_DB,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 20000,
});

pgPool.on('error', (err, client) => {
    console.error('PG Pool: Unexpected error on idle client', err)
    process.exit(-1)
})

const client = new AthenaClient({
    region: AWS_REGION,
    credentials: fromEnv()
});

const glueTableNameList = ['glue_table1', 'glue_table2', 'glue_table3']

let tablesWithColumns = [];
//Athena way - but ordered alphabetically. Solution is to drop NOT NULL for all FKs and then re-add them later
// let getDatabaseCommand = new ListTableMetadataCommand({CatalogName: CATALOG_NAME, DatabaseName: DB_NAME});
// let getDatabaseResponse = await client.send(getDatabaseCommand);
// tablesWithColumns = getDatabaseResponse.TableMetadataList.map(table => ({
//     name: table.Name,
//     pgName: table.Name.substring(6),
//     columns: table.Columns.map(column => column.Name)
// }))

async function insertTablesInOrder() {
    tablesWithColumns = glueTableNameList.map(async tableName => {
        const getTableMetadataCommand = new GetTableMetadataCommand({
            CatalogName: CATALOG_NAME,
            DatabaseName: DB_NAME,
            TableName: tableName
        })
        const response = await client.send(getTableMetadataCommand);
        return {
            athName: tableName,
            pgName: tableName.substring(6),
            columns: response.TableMetadata.Columns.map(column => column.Name)
        }
    })
    tablesWithColumns = await Promise.all(tablesWithColumns);
    for (const table of tablesWithColumns) {
        await queryAthena(table.athName, table.pgName, table.columns);
    }
}


async function queryAthena(athTableName, pgTableName, columns) {
    console.log(`---------------------- Querying Athena for ${athTableName} ----------------------`);
    const startQueryInput = {
        QueryString: `SELECT ${columns.join(', ')} FROM ${athTableName}`,
        QueryExecutionContext: {
            Database: DB_NAME,
            Catalog: CATALOG_NAME,
        },
        ResultConfiguration: {
            OutputLocation: ATHENA_OUTPUT_S3_BUCKET
        },
        ResultReuseConfiguration: {
            ResultReuseByAgeConfiguration: {
                Enabled: true
            },
        },
    };
    const startCommand = new StartQueryExecutionCommand(startQueryInput);
    const startCommandResponse = await client.send(startCommand);

    while (true) {
        const getQueryExecutionCommand = new GetQueryExecutionCommand({QueryExecutionId: startCommandResponse.QueryExecutionId});
        const response = await client.send(getQueryExecutionCommand);
        if (response.QueryExecution.Status.State === 'FAILED') {
            console.error("Query Failed. ", response.QueryExecution.Status.StateChangeReason)
            break;
        }
        if (response.QueryExecution.Status.State === 'SUCCEEDED') {
            console.log("Query Succeeded. Now fetching results...")
            let nextToken;
            do {
                const getQueryResultsCommand = new GetQueryResultsCommand({
                    QueryExecutionId: startCommandResponse.QueryExecutionId,
                    NextToken: nextToken
                });
                const resultsResponse = await client.send(getQueryResultsCommand);
                let rows = [];
                if (!nextToken) { //first row or first page is column names
                    // console.log(resultsResponse.ResultSet.Rows[0])
                    rows = resultsResponse.ResultSet.Rows.slice(1).map(row => row.Data.map(data => data.VarCharValue));
                } else
                    rows = rows.concat(resultsResponse.ResultSet.Rows.map(row => row.Data.map(data => data.VarCharValue)));
                nextToken = resultsResponse.NextToken;
                await insertIntoPostgresAsBatch(pgTableName, columns, rows)
            } while (nextToken)

            break;
        }
        await new Promise(resolve => setTimeout(resolve, 5000));
    }
    console.log(`---------------------- Finished querying Athena for ${athTableName} ---------------------- \n`);
}

async function insertIntoPostgresAsBatch(tableName, columns, rows) {
    console.log(`Number of rows inserting to ${tableName}: ${rows.length}`);
    const columnNames = columns.join(', ');
    const columnsCount = rows[0]?.length;
    const values = [];
    const valuePlaceholders = rows.map((row, rowIndex) => {
        const rowPlaceholders = row.map((_, colIndex) => `$${rowIndex * columnsCount + colIndex + 1}`).join(', ');
        values.push(...row);
        return `(${rowPlaceholders})`;
    }).join(', ');
    const query = `INSERT INTO ${DB_SCHEMA}.${tableName} (${columnNames}) VALUES ${valuePlaceholders}`;
    const client = await pgPool.connect();
    try {
        const response = await client.query(query, values);
        console.log(`Inserted ${response.rowCount} rows into ${tableName}`);
    } catch (error) {
        console.error(`Error while inserting into Postgres table ${tableName}`, error);
    } finally {
        client.release();
    }
}

/**
 * Insert rows into Postgres one by one. This is expensive and slow.
 * @param tableName PG table name
 * @param columns PG columns
 * @param rows Rows to insert
 */
async function insertIntoPostgres(tableName, columns, rows) {
    console.log(`Number of rows inserting to ${tableName}: ${rows.length}`);
    for (const row of rows) {
        const placeholders = columns.map((_, idx) => `$${idx + 1}`).join(', ');
        const columnNames = columns.join(', ');
        const query = `INSERT INTO ${DB_SCHEMA}.${tableName} (${columnNames}) VALUES (${placeholders})`;
        const client = await pgPool.connect();
        try {
            await client.query(query, row);
        } catch (error) {
            console.error("Error while inserting into Postgres", error);
        } finally {
            client.release();
        }
    }
    console.log(`Inserted ${rows.length} rows into ${tableName}`);
}


const startTime = Date.now();
await insertTablesInOrder();
const endTime = Date.now();
const duration = (endTime - startTime) / 1000; // duration in seconds
console.log(`Script completed in ${duration} seconds.`);

//node athena-to-postgres.js >> /Users/putanwar/Personal/pet-projects/pg_insertion.log &