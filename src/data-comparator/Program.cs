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
    NormalizeMongoFields(mongoData);
    File.WriteAllText(mongoFilePath, mongoData.ToString());
        
    var sqlData = JArray.Parse(File.ReadAllText(sqlFilePath));
    NormalizeSqlFields(sqlData);
    File.WriteAllText(sqlFilePath, sqlData.ToString());

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

        var recordDifferences = CompareRecords(mongoRecord, sqlRecord);
        differences.Add(recordDifferences);
    }

    entityReport["Differences"] = differences;
    report[collectionName] = entityReport;
}

JObject CompareRecords(JToken mongoRecord, JToken sqlRecord)
{
    var mongoFields = mongoRecord.Children<JProperty>().Select(p => p.Name).ToHashSet();
    var sqlFields = sqlRecord.Children<JProperty>().Select(p => p.Name).ToHashSet();

    var fieldsOnlyInMongo = mongoFields.Except(sqlFields).ToList();
    var fieldsOnlyInSql = sqlFields.Except(mongoFields).ToList();

    var recordDifferences = new JObject
    {
        ["FieldsOnlyInMongo"] = JToken.FromObject(fieldsOnlyInMongo),
        ["FieldsOnlyInSql"] = JToken.FromObject(fieldsOnlyInSql)
    };

    var commonMongoFields = mongoFields.Except(fieldsOnlyInMongo).ToList();
    var commonSqlFields = sqlFields.Except(fieldsOnlyInSql).ToList();
    var combinedCommonFields = commonMongoFields.Intersect(commonSqlFields).ToList();

    var fieldDifferences = new JArray();

    foreach (var field in combinedCommonFields)
    {
        if (field == "Parameters" || field == "IntegrationParameters")
        {
            var parameterDifferences = CompareComplexField(mongoRecord[field]!, sqlRecord[field]!);
            if (parameterDifferences.Count > 0)
            {
                fieldDifferences.Add(new JObject
                {
                    ["Field"] = field,
                    ["ParameterDifferences"] = parameterDifferences
                });
            }
        }
        else
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
    }

    recordDifferences["FieldDifferences"] = fieldDifferences;
    return recordDifferences;
}

JArray CompareComplexField(JToken mongoField, JToken sqlField)
{
    if (mongoField.Type == JTokenType.Array && sqlField.Type == JTokenType.Array)
    {
        return CompareParameters(mongoField, sqlField);
    }
    else if (mongoField.Type == JTokenType.Object && sqlField.Type == JTokenType.Object)
    {
        return CompareObjectFields(mongoField, sqlField);
    }
    else
    {
        return
        [
            new JObject
            {
                ["MongoValue"] = mongoField,
                ["SqlValue"] = sqlField
            }
        ];
    }
}

JArray CompareParameters(JToken mongoParameters, JToken sqlParameters)
{
    var mongoArray = (JArray)mongoParameters;
    var sqlArray = (JArray)sqlParameters;

    var parameterDifferences = new JArray();

    for (int i = 0; i < Math.Max(mongoArray.Count, sqlArray.Count); i++)
    {
        var mongoParameter = i < mongoArray.Count ? mongoArray[i] : null;
        var sqlParameter = i < sqlArray.Count ? sqlArray[i] : null;

        if (mongoParameter == null || sqlParameter == null)
        {
            parameterDifferences.Add(new JObject
            {
                ["Index"] = i,
                ["MongoParameter"] = mongoParameter,
                ["SqlParameter"] = sqlParameter
            });
            continue;
        }

        var parameterFieldDifferences = CompareObjectFields(mongoParameter, sqlParameter);
        parameterDifferences.Add(new JObject
        {
            ["Index"] = i,
            ["ParameterFieldDifferences"] = parameterFieldDifferences
        });
    }

    return parameterDifferences;
}

JArray CompareObjectFields(JToken mongoObject, JToken sqlObject)
{
    var mongoFields = mongoObject.Children<JProperty>().Select(p => p.Name).ToHashSet();
    var sqlFields = sqlObject.Children<JProperty>().Select(p => p.Name).ToHashSet();

    var fieldsOnlyInMongo = mongoFields.Except(sqlFields).ToList();
    var fieldsOnlyInSql = sqlFields.Except(mongoFields).ToList();

    var fieldDifferences = new JArray();

    var commonMongoParametersFields = mongoFields.Except(fieldsOnlyInMongo).ToList();
    var commonSqlParametersFields = sqlFields.Except(fieldsOnlyInSql).ToList();
    var combinedCommonParametersFields = commonMongoParametersFields.Intersect(commonSqlParametersFields).ToList();

    foreach (var field in combinedCommonParametersFields)
    {
        var mongoValue = mongoObject[field]!.ToString().ToLowerInvariant();
        var sqlValue = sqlObject[field]!.ToString().ToLowerInvariant();

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

    return
    [
        new JObject
        {
            ["FieldsOnlyInMongo"] = JToken.FromObject(fieldsOnlyInMongo),
            ["FieldsOnlyInSql"] = JToken.FromObject(fieldsOnlyInSql),
            ["FieldDifferences"] = fieldDifferences
        }
    ];
}

void NormalizeSqlFields(JArray sqlData)
{
    foreach (var record in sqlData)
    {
        NormalizeFields(record, "Parameters");
        NormalizeFields(record, "Data");

        if (record["Parameters"] != null)
        {
            foreach (var parameter in record["Parameters"]!)
            {
                NormalizeFields(parameter, "_id");
            }
        }
    }
}

void NormalizeMongoFields(JArray mongoData)
{
    foreach (var record in mongoData)
    {
        NormalizeFields(record, "_id");
        NormalizeFields(record, "SellerId");

        SetIdField(record);
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
            var fieldValue = record[fieldName]!.ToString();
            var parsedValue = JToken.Parse(fieldValue);
            record[fieldName] = parsedValue;
        }
    }
}

Dictionary<string, int> GetDuplicateRecords(JArray data, string fieldToCompare)
{
    return data.GroupBy(x => x[fieldToCompare].ToString())
               .Where(g => g.Count() > 1)
               .ToDictionary(g => g.Key, g => g.Count());
}
