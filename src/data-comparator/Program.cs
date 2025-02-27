using Newtonsoft.Json.Linq;

var sellersMongoJsonFilePath = @"filePath";
var intParamsMongoJsonFilePath = @"filePath";

var sellersSqlJsonFilePath = @"filePath";
var intParamsSqlJsonFilePath = @"filePath";

CompareJsonFiles(sellersMongoJsonFilePath, sellersSqlJsonFilePath, "cnpj", "Sellers");
CompareJsonFiles(intParamsMongoJsonFilePath, intParamsSqlJsonFilePath, "sellerId", "IntegrationParameters");

void CompareJsonFiles(string mongoFilePath, string sqlFilePath, string uniqueField, string collectionName)
{
    var mongoData = JArray.Parse(File.ReadAllText(mongoFilePath));
    NormalizeMongoIds(mongoData);
    File.WriteAllText(mongoFilePath, mongoData.ToString());

    var sqlJson = JObject.Parse(File.ReadAllText(sqlFilePath));
    var sqlData = (JArray)sqlJson[collectionName]!;

    Console.WriteLine($"Entity: {collectionName} - Mongo records count: {mongoData.Count}");
    Console.WriteLine($"Entity: {collectionName} - SQL records count: {sqlData.Count}");

    var mongoDuplicates = GetDuplicateRecords(mongoData, uniqueField);
    var sqlDuplicates = GetDuplicateRecords(sqlData, uniqueField);

    Console.WriteLine($"Entity: {collectionName} - Mongo duplicate records:");
    foreach (var duplicate in mongoDuplicates)
    {
        Console.WriteLine($"{duplicate.Key} : {duplicate.Value}");
    }

    Console.WriteLine($"Entity: {collectionName} - SQL duplicate records:");
    foreach (var duplicate in sqlDuplicates)
    {
        Console.WriteLine($"{duplicate.Key} : {duplicate.Value}");
    }

    var mongoIds = new HashSet<string>(mongoData.Select(x => x[uniqueField].ToString()));
    var sqlIds = new HashSet<string>(sqlData.Select(x => x[uniqueField].ToString()));

    var onlyInMongo = mongoIds.Except(sqlIds).ToList();
    var onlyInSql = sqlIds.Except(mongoIds).ToList();

    Console.WriteLine($"Entity: {collectionName} - Records only in Mongo ({onlyInMongo.Count}): {string.Join(", ", onlyInMongo)}");
    Console.WriteLine($"Entity: {collectionName} - Records only in SQL ({onlyInSql.Count}): {string.Join(", ", onlyInSql)}");

    var commonIds = mongoIds.Intersect(sqlIds).ToList();

    foreach (var id in commonIds)
    {
        var mongoRecord = mongoData.First(x => x[uniqueField].ToString() == id);
        var sqlRecord = sqlData.First(x => x[uniqueField].ToString() == id);

        var mongoFields = mongoRecord.Children<JProperty>().Select(p => p.Name).ToHashSet();
        var sqlFields = sqlRecord.Children<JProperty>().Select(p => p.Name).ToHashSet();

        var fieldsOnlyInMongo = mongoFields.Except(sqlFields).ToList();
        var fieldsOnlyInSql = sqlFields.Except(mongoFields).ToList();

        Console.WriteLine($"Entity: {collectionName} - Fields only in Mongo for {uniqueField} {id}: {string.Join(", ", fieldsOnlyInMongo)}");
        Console.WriteLine($"Entity: {collectionName} - Fields only in SQL for {uniqueField} {id}: {string.Join(", ", fieldsOnlyInSql)}");

        var commonFields = mongoFields.Intersect(sqlFields).ToList();

        foreach (var field in commonFields)
        {
            var mongoValue = mongoRecord[field].ToString();
            var sqlValue = sqlRecord[field].ToString();

            if (mongoValue != sqlValue)
            {
                Console.WriteLine($"Entity: {collectionName} - Difference in field '{field}' for {uniqueField} {id}: Mongo='{mongoValue}', SQL='{sqlValue}'");
            }
        }
    }
}

void NormalizeMongoIds(JArray mongoData)
{
    foreach (var record in mongoData)
    {
        NormalizeIdField(record, "_id");
        NormalizeIdField(record, "PaymentForms");
        NormalizeIdField(record, "SellerId");
    }
}

void NormalizeIdField(JToken record, string fieldName)
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
                NormalizeIdField(item, "_id");
            }
        }
    }
}

Dictionary<string, int> GetDuplicateRecords(JArray data, string uniqueField)
{
    return data.GroupBy(x => x[uniqueField].ToString())
               .Where(g => g.Count() > 1)
               .ToDictionary(g => g.Key, g => g.Count());
}
