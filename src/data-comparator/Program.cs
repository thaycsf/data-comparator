using Newtonsoft.Json.Linq;

var sellersMongoJsonFilePath = @"C:\Users\thaynacsf\OneDrive - Votorantim\Documentos\_Repositories\data-comparator\documents\jsmcache-hml.sellers.json";
var intParamsMongoJsonFilePath = @"C:\Users\thaynacsf\OneDrive - Votorantim\Documentos\_Repositories\data-comparator\documents\jsmcache-hml.sellerIntegrationParams.json";

var sellersSqlJsonFilePath = @"C:\Users\thaynacsf\OneDrive - Votorantim\Documentos\_Repositories\data-comparator\documents\sql-hml-sellers.json";
var intParamsSqlJsonFilePath = @"C:\Users\thaynacsf\OneDrive - Votorantim\Documentos\_Repositories\data-comparator\documents\sql-integrationParameters.json";

CompareJsonFiles(sellersMongoJsonFilePath, sellersSqlJsonFilePath, "cnpj");
CompareJsonFiles(intParamsMongoJsonFilePath, intParamsSqlJsonFilePath, "sellerId");

void CompareJsonFiles(string mongoFilePath, string sqlFilePath, string uniqueField)
{
    var mongoData = JArray.Parse(File.ReadAllText(mongoFilePath));
    NormalizeMongoIds(mongoData);
    File.WriteAllText(mongoFilePath, mongoData.ToString());

    var sqlData = JArray.Parse(File.ReadAllText(sqlFilePath));

    Console.WriteLine($"Mongo records count: {mongoData.Count}");
    Console.WriteLine($"SQL records count: {sqlData.Count}");

    var mongoDuplicates = GetDuplicateRecords(mongoData, uniqueField);
    var sqlDuplicates = GetDuplicateRecords(sqlData, uniqueField);

    Console.WriteLine("Mongo duplicate records:");
    foreach (var duplicate in mongoDuplicates)
    {
        Console.WriteLine($"{duplicate.Key} : {duplicate.Value}");
    }

    Console.WriteLine("SQL duplicate records:");
    foreach (var duplicate in sqlDuplicates)
    {
        Console.WriteLine($"{duplicate.Key} : {duplicate.Value}");
    }

    var mongoIds = new HashSet<string>(mongoData.Select(x => x[uniqueField].ToString()));
    var sqlIds = new HashSet<string>(sqlData.Select(x => x[uniqueField].ToString()));

    var onlyInMongo = mongoIds.Except(sqlIds).ToList();
    var onlyInSql = sqlIds.Except(mongoIds).ToList();

    Console.WriteLine($"Records only in Mongo ({onlyInMongo.Count}): {string.Join(", ", onlyInMongo)}");
    Console.WriteLine($"Records only in SQL ({onlyInSql.Count}): {string.Join(", ", onlyInSql)}");

    var commonIds = mongoIds.Intersect(sqlIds).ToList();

    foreach (var id in commonIds)
    {
        var mongoRecord = mongoData.First(x => x[uniqueField].ToString() == id);
        var sqlRecord = sqlData.First(x => x[uniqueField].ToString() == id);

        var mongoFields = mongoRecord.Children<JProperty>().Select(p => p.Name).ToHashSet();
        var sqlFields = sqlRecord.Children<JProperty>().Select(p => p.Name).ToHashSet();

        var fieldsOnlyInMongo = mongoFields.Except(sqlFields).ToList();
        var fieldsOnlyInSql = sqlFields.Except(mongoFields).ToList();

        Console.WriteLine($"Fields only in Mongo for {uniqueField} {id}: {string.Join(", ", fieldsOnlyInMongo)}");
        Console.WriteLine($"Fields only in SQL for {uniqueField} {id}: {string.Join(", ", fieldsOnlyInSql)}");

        var commonFields = mongoFields.Intersect(sqlFields).ToList();

        foreach (var field in commonFields)
        {
            var mongoValue = mongoRecord[field].ToString();
            var sqlValue = sqlRecord[field].ToString();

            if (mongoValue != sqlValue)
            {
                Console.WriteLine($"Difference in field '{field}' for {uniqueField} {id}: Mongo='{mongoValue}', SQL='{sqlValue}'");
            }
        }
    }
}

void NormalizeMongoIds(JArray mongoData)
{
    foreach (var record in mongoData)
    {
        if (record["_id"] != null && record["_id"].Type == JTokenType.Object)
        {
            var idObject = (JObject)record["_id"];
            if (idObject["$binary"] != null)
            {
                var base64String = idObject["$binary"]["base64"].ToString();
                var bytes = Convert.FromBase64String(base64String);
                var uuid = new Guid(bytes);
                record["_id"] = uuid.ToString();
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
