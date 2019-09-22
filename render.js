const fs       = require("fs")
const jsYaml   = require("js-yaml")
const mustache = require("mustache")
const util     = require("util")

const readFile  = util.promisify(fs.readFile)
const writeFile = util.promisify(fs.writeFile)

function defineColumn(name, type, isNullable, isPrimaryKey) {
  return `${name} ${type} ${isNullable ? '' : 'NOT '}NULL${isPrimaryKey ? ' PRIMARY KEY' : ''}`
}

function defineTableBody(columns, primaryKeys) {
  columns     = columns.map(([name, type, isNullable, isPrimaryKey]) => defineColumn(name, type, isNullable, isPrimaryKey) + ",")
  primaryKeys = `PRIMARY KEY (${primaryKeys.join(", ")})`
  return columns.concat(primaryKeys)
}

function renderQuery(template, query) {
  const values = {
    table_name:         query.table_name,
    table_alias:        query.table_alias,
    staging_table_name: query.staging_table_name,

    staging_table_body: () => {
      const columns = query.fields.map(column => {
        column    = [...column]
        column[0] = column[0].replace(/^[^.]*\./, "")
        column[3] = false
        return column
      })

      return defineTableBody(columns, query.primary_key)
    },

    fields: () => {
      const head   = query.fields.slice(0, -1).map(([field]) => field + ",")
      const [tail] = query.fields[query.fields.length - 1]
      return head.concat(tail)
    },

    stg_fields: () => {
      const fields = query.fields.map(([field]) => field.replace(/^[^.]*\./, ""))
      const head   = fields.slice(0, -1).map(field => field + ",")
      const tail   = fields[fields.length - 1]
      return head.concat(tail)
    },

    joins: () => {
      if (!("joins" in query)) return ""

      return query.joins
        .map(([name, alias, conditions]) => {
          conditions = conditions.join("\nAND ")
          return `INNER JOIN ${name} AS ${alias} ON ${conditions}`
        })
        .join("\n")
    },

    conditions: () => {
      return query.conditions.join("\nAND ")
    },

    values: () => {
      const head = query.values.slice(0, -1).map(field => field + ",")
      const tail = query.values[query.values.length - 1]
      return head.concat(tail)
//      return query.values.join(",\n")
    },

    stg_join_conditions: () => {
      return query.stg_join_conditions
        .map(([tbl, stg]) => `tbl.${tbl} = stg.${stg}`)
        .join("\nAND ")
    },
  }

  return mustache.render(template, values)
}

;(async () => {
  try {
    /** @todo YAML syntax check; remove this */
    //jsYaml.safeLoad(await readFile("C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\query\\2.yml"))

    const updateTemplate = await readFile(`${__dirname}/update.tpl.sql`, "utf8")

    const queriesTemplateMappings = [
      [
        "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\query\\hotel-contacts.yml",
        "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\template\\hotel-contacts.ms.sql",
        "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\sp\\dbo.ycs41_mask_pii_hotel_contact_details_v1.sql",
      ],
      [
        "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\query\\hotel-feedback.yml",
        "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\template\\hotel-feedback.ms.sql",
        "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\sp\\dbo.ycs41_mask_pii_hotel_feedback_details_v1.sql",
      ],
      [
        "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\query\\hotel-ec-contact.yml",
        "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\template\\hotel-ec-contact.ms.sql",
        "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\sp\\dbo.ycs41_mask_pii_in_hotel_ec_contact_details_v1.sql",
      ],
      [
        "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\query\\hotel-security.yml",
        "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\template\\hotel-security.ms.sql",
        "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\sp\\dbo.ycs41_mask_pii_in_security_details_v1.sql",
      ],
      // [
      //   "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\query\\hotel-bank-account.yml",
      //   "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\template\\hotel-bank-account.ms.sql",
      //   "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\sp\\dbo.ycs41_mask_pii_hotel_bank_account_details_v1.sql",
      // ],
      // [
      //   "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\query\\hotel-channel-manager.yml",
      //   "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\template\\hotel-channel-manager.ms.sql",
      //   "",
      // ],
      // [
      //   "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\query\\hotel-auth-hot.yml",
      //   "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\template\\hotel-auth-hot.ms.sql",
      //   "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\sp\\dbo.ycs41_mask_pii_in_auth_hot_details_v1.sql",
      // ],
      // [
      //   "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\query\\ycs3.yml",
      //   "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\template\\ycs3.ms.sql",
      //   "C:\\Users\\cvidal\\git\\cvidal\\cvidal\\wipe-ycs-pii\\sp\\dbo.ycs41_mask_pii_in_ycs3_details_v1.sql",
      // ],
    ]

    let promises = queriesTemplateMappings.map(([queries, template, sp]) => Promise.all([
      readFile(queries).then(queries => jsYaml.safeLoad(queries)),
      readFile(template, "utf8"),
      sp,
    ]))

    promises = promises.map(async results => {
      const [queries, template, sp] = await results

      let sql = queries
        .map(query => renderQuery(updateTemplate, query))
        .map(
          query =>
            query
              .split("\n")
              .map(
                line => {
                  if (line.trim() == "") return line
                  return " ".repeat(8).concat(line)
                }
              )
              .join("\n")
        )
        .join("\n")

      sql = mustache.render(template, {sql})
      await writeFile(sp, sql)
    })

    await Promise.all(promises)

/*    const contents = await Promise.all(promises)

    contents.forEach(([queries, template]) => {
      queries = jsYaml.safeLoad(queries)

      const sql = queries
        .map(query => renderQuery(updateTemplate, query))
        .join("\n")

      console.log(mustache.render(template, {sql}))
    })*/
  } catch (error) {
    console.error(error)
  }
})()
