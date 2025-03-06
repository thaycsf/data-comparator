using Newtonsoft.Json.Linq;

var sellersMongoJsonFilePath = @"filePath";
var intParamsMongoJsonFilePath = @"filePath";

var sellersSqlJsonFilePath = @"filePath";
var intParamsSqlJsonFilePath = @"filePath";

var comparisonReport = new JObject();

CompareJsonFiles(sellersMongoJsonFilePath, sellersSqlJsonFilePath, "CNPJ", "Sellers", comparisonReport);
CompareJsonFiles(intParamsMongoJsonFilePath, intParamsSqlJsonFilePath, "SellerId", "IntegrationParameters", comparisonReport);

File.WriteAllText("comparison_report.json", comparisonReport.ToString());

void CompareJsonFiles(string mongoFilePath, string sqlFilePath, string fieldToQuery, string collectionName, JObject report)
{
    var mongoData = JArray.Parse(File.ReadAllText(mongoFilePath));
    NormalizeMongoIds(mongoData);
    File.WriteAllText(mongoFilePath, mongoData.ToString());

    var sqlJson = JObject.Parse(File.ReadAllText(sqlFilePath));
    var sqlData = (JArray)sqlJson[collectionName]!;

    var entityReport = new JObject
    {
        ["MongoRecordsCount"] = mongoData.Count,
        ["SqlRecordsCount"] = sqlData.Count
    };

    var mongoDuplicates = GetDuplicateRecords(mongoData, fieldToQuery);
    var sqlDuplicates = GetDuplicateRecords(sqlData, fieldToQuery);

    entityReport[$"MongoDuplicateRecords-{fieldToQuery}"] = JToken.FromObject(mongoDuplicates);
    entityReport[$"SqlDuplicateRecords-{fieldToQuery}"] = JToken.FromObject(sqlDuplicates);

    fieldToQuery = collectionName == "Sellers" ? "Id" : fieldToQuery;

    var mongoIds = new HashSet<string>(mongoData.Select(x => x[fieldToQuery].ToString().ToLowerInvariant()));
    var sqlIds = new HashSet<string>(sqlData.Select(x => x[fieldToQuery].ToString().ToLowerInvariant()));

    var onlyInMongo = mongoIds.Except(sqlIds).ToList();
    var onlyInSql = sqlIds.Except(mongoIds).ToList();

    entityReport["RecordsOnlyInMongo"] = JToken.FromObject(onlyInMongo);
    entityReport["RecordsOnlyInSql"] = JToken.FromObject(onlyInSql);

    var commonMongoIds = mongoIds.Except(onlyInMongo).ToList();
    var commonSqlIds = sqlIds.Except(onlyInSql).ToList();
    var combinedCommonIds = commonMongoIds.Intersect(commonSqlIds).ToList();

    var differences = new JArray();

    foreach (var id in combinedCommonIds)
    {
        var mongoRecord = mongoData.First(x => x[fieldToQuery].ToString().ToLowerInvariant() == id);
        var sqlRecord = sqlData.First(x => x[fieldToQuery].ToString().ToLowerInvariant() == id);

        var mongoFields = mongoRecord.Children<JProperty>().Select(p => p.Name).ToHashSet();
        var sqlFields = sqlRecord.Children<JProperty>().Select(p => p.Name).ToHashSet();

        var fieldsOnlyInMongo = mongoFields.Except(sqlFields).ToList();
        var fieldsOnlyInSql = sqlFields.Except(mongoFields).ToList();

        var recordDifferences = new JObject
        {
            ["Id"] = id,
            ["FieldsOnlyInMongo"] = JToken.FromObject(fieldsOnlyInMongo),
            ["FieldsOnlyInSql"] = JToken.FromObject(fieldsOnlyInSql)
        };

        var combinedCommonFields = mongoFields.Intersect(sqlFields).ToList();

        var fieldDifferences = new JArray();

        foreach (var field in combinedCommonFields)
        {
            var mongoValue = mongoRecord[field]!.ToString().ToLowerInvariant();
            var sqlValue = sqlRecord[field]!.ToString().ToLowerInvariant();

            if (mongoValue != sqlValue)
            {
                fieldDifferences.Add(new JObject
                {
                    ["Field"] = field,
                    ["MongoValue"] = mongoValue,
                    ["SqlValue"] = sqlValue
                });
            }
        }

        recordDifferences["FieldDifferences"] = fieldDifferences;
        differences.Add(recordDifferences);
    }

    entityReport["Differences"] = differences;
    report[collectionName] = entityReport;
}

void NormalizeMongoIds(JArray mongoData)
{
    foreach (var record in mongoData)
    {
        NormalizeFields(record, "_id");
        NormalizeFields(record, "SellerId");
        NormalizeFields(record, "Parameters");
        NormalizeFields(record, "Data");

        SetIdField(record);

        if (record["Parameters"] != null)
        {
            foreach (var parameter in record["Parameters"]!)
            {
                SetIdField(parameter);
            }
        }
    }
}

void SetIdField(JToken record)
{
    if (record["_id"] != null)
    {
        record["Id"] = record["_id"];
        ((JObject)record).Remove("_id");
    }
}

void NormalizeFields(JToken record, string fieldName)
{
    if (record[fieldName] != null)
    {
        if (record[fieldName]!.Type == JTokenType.Object)
        {
            var idObject = (JObject)record[fieldName];
            if (idObject["$binary"] != null)
            {
                var base64String = idObject["$binary"]["base64"].ToString();
                var bytes = Convert.FromBase64String(base64String);
                var guid = new Guid(bytes);
                record[fieldName] = guid.ToString();
            }
        }
        else if (record[fieldName]!.Type == JTokenType.Array)
        {
            foreach (var item in record[fieldName]!)
            {
                NormalizeFields(item, "_id");
            }
        }
        else if (record[fieldName]!.Type == JTokenType.String && (fieldName == "Parameters" || fieldName == "Data"))
        {
            var parametersArray = JArray.Parse(record[fieldName]!.ToString());
            record[fieldName] = parametersArray;
        }
    }
}

Dictionary<string, int> GetDuplicateRecords(JArray data, string fieldToCompare)
{
    return data.GroupBy(x => x[fieldToCompare].ToString())
               .Where(g => g.Count() > 1)
               .ToDictionary(g => g.Key, g => g.Count());
}
