const fs       = require("fs")
const mssql    = require("mssql")
const readline = require("readline")
const util     = require("util")

async function enumTables(pool, database) {
  const result = await pool
    .request()
    .query(`
      SELECT
        TABLE_SCHEMA,
        TABLE_NAME
      FROM information_schema.tables
      WHERE
        TABLE_CATALOG = '${database}'
        AND TABLE_TYPE = 'BASE TABLE'
    `)

  return result.recordset.map(({TABLE_SCHEMA, TABLE_NAME}) => [TABLE_SCHEMA, TABLE_NAME])
}

async function enumColumns(pool, database, table) {
  const result = await pool
    .request()
    .query(`
      SELECT *
      FROM information_schema.columns
      WHERE
        TABLE_CATALOG = '${database}'
        AND TABLE_NAME = '${table}'
    `)

  return result.recordset.map(({COLUMN_NAME}) => COLUMN_NAME)
}

async function selectSamples(pool, database, table, column) {
  const result = await pool
    .request()
    .query(`
      SELECT TOP 5 value
      FROM (
        SELECT DISTINCT ${column} AS value
        FROM ${table}
      ) AS t
    `)

  return result.recordset.map(({value}) => value)
}

async function chooseSingle(choice, choices) {
  const valuesByIndex = {}
  choices.forEach(([value], index) => valuesByIndex[index] = value)
  choices = choices.map(([_, label], index) => `${index + 1}: ${label}`).join("\n")
  let done = false

  const rl = readline.createInterface({
    input:  process.stdin,
    output: process.stdout,
  })

  while (true) {
    try {
      const value = await new Promise((resolve, reject) => {
        rl.question(`${choice}\n${choices}\n`, answer => {
          const index = answer - 1

          if (index in valuesByIndex) {
            const value = valuesByIndex[index]
            return resolve(value)
          }

          const error = new Error(`${answer} is not a valid choice`)
          reject(error)
        })
      })

      rl.close()
      return value
    } catch (error) {
      console.log(`ERROR: ${error.message}`)
    }
  }
}

const PII_EMAIL = 1
const PII_PHONE = 2

const piiTypes = [
  [PII_EMAIL, "E-mail address"],
  [PII_PHONE, "Telephone number"],
]

async function processTable(output, pool, database, table) {
  console.log(`--- Table ${table} ---`)
  const cols    = await enumColumns(pool, database, table)
  const columns = []

  for (const c of cols) {
    console.log(`Column: ${c}`)
    const samples = await selectSamples(pool, database, table, c)
    console.log(`Sample values: ${samples.join(", ")}`)
    const answer = await chooseSingle(`Any PII?`, piiTypes)
    console.log(`You chose: ${answer}`)

    columns.push({
      name: c,
      pii:  [answer],
    })
  }

  output.push({
    name:    table,
    columns: columns,
  })

  await util.promisify(fs.writeFile)(`${__dirname}/output.json`, JSON.stringify(output, null, 2))
}

(async () => {
  let pool

  try {
    const input      = JSON.parse(await util.promisify(fs.readFile)(`${__dirname}/input.json`))
    const tablesDone = new Set
    input.forEach(({name}) => tablesDone.add(name))

    const output = input
    const host   = process.env['MSSQL_HOST']
    pool         = await mssql.connect(`mssql://SA:TheSaPassword!@${host}/db`)
    const tables = await enumTables(pool, "db")

    for (const [_, table] of tables) {
      if (tablesDone.has(table)) {
        console.log(`--- Table ${table} (skipped) ---`)
        continue
      }

      await processTable(output, pool, "db", table)
    }
    console.log("DONE")
  } catch (error) {
    console.error(error)
  } finally {
    if (pool) await pool.close()
  }
})()
