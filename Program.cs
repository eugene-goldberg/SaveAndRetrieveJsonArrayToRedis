using Newtonsoft.Json;
using StackExchange.Redis;
using System.Diagnostics;
using System.Text;

namespace RedisJsonExample
{
    class Customer
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string City { get; set; }
        public string Email { get; set; }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            var connectionMultiplexer = ConnectionMultiplexer.Connect("localhost");
            var db = connectionMultiplexer.GetDatabase();
            var redisKey = "customers";

           // await InsertCustomers(db);
            await GetCustomers(connectionMultiplexer, db);

            connectionMultiplexer.Close();
        }

        private static async Task InsertCustomers(IDatabase db)
        {
            var numCustomers = 1000000;
            var tasks = new List<Task>(numCustomers);

            var stopwatch = new Stopwatch();
            stopwatch.Start();
            
            for (int i = 1; i <= numCustomers; i++)
            {
                var customer = new Customer
                {
                    Id = i,
                    Name = $"Customer {i}",
                    City = $"City {i % 50}", // Example: 50 unique cities
                    Email = $"customer{i}@example.com"
                };

                var json = JsonConvert.SerializeObject(customer);
                var redisKey = $"customer:{i}";
                tasks.Add(db.ExecuteAsync("JSON.SET", redisKey, ".", json));
            }

            await Task.WhenAll(tasks);

            stopwatch.Stop();

            Console.WriteLine($"Inserted 1000000 customers in {stopwatch.ElapsedMilliseconds} ms.");
        }
        //Get all customers
        // private static async Task GetCustomers(IDatabase db, string redisKey)
        // {
        //      var stopwatch = new Stopwatch();
        //      stopwatch.Start();

        //     var redisValue = await db.ExecuteAsync("JSON.GET", redisKey);
        //     if (!redisValue.IsNull)
        //     {
        //         // Get the string representation of the RedisResult
        //         string json = redisValue.ToString();

        //         // Deserialize the JSON string into a Customer object
        //         var retrievedCustomer = JsonConvert.DeserializeObject<Customer>(json);
        //         stopwatch.Stop();

        //         Console.WriteLine($"Retrieved 1000000 customers in {stopwatch.ElapsedMilliseconds} ms.");

        //         // Print the retrieved customer object
        //         // var customer = retrievedCustomer[0];
        //          Console.WriteLine($" Retrieved Customer: " + retrievedCustomer.ToString());
        //         // Console.WriteLine($"Id: {retrievedCustomer.Id}");
        //     }
        // }

      public static async Task<List<Customer>> GetAllCustomersAsync(IConnectionMultiplexer multiplexer, IDatabase db)
    {
        var server = multiplexer.GetServer(multiplexer.GetEndPoints().First());
        var keys = server.Keys(pattern: "customer:*").Select(k => k.ToString()).ToArray();

        var tasks = keys.Select(key => db.ExecuteAsync("JSON.GET", key)).ToArray();

        await Task.WhenAll(tasks);

        var customers = tasks
            .Where(t => !t.Result.IsNull)
            .Select(t => JsonConvert.DeserializeObject<Customer>(t.Result.ToString()))
            .ToList();

        return customers;
    }

     public static async Task GetCustomers(IConnectionMultiplexer _multiplexer, IDatabase db)
    {
        var stopwatch = Stopwatch.StartNew();
        var allCustomers = await GetAllCustomersAsync(_multiplexer, db);
        stopwatch.Stop();
        var elapsedMilliseconds = stopwatch.ElapsedMilliseconds;
        Console.WriteLine($"Fetched all customers. Total count: {allCustomers.Count}");
        Console.WriteLine($"GetAllCustomersAsync took {elapsedMilliseconds} milliseconds.");
        Console.WriteLine(JsonConvert.SerializeObject(allCustomers[9999], Formatting.Indented));
    }
    }
}

