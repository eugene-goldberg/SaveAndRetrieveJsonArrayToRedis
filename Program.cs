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
            await UpdateCustomer(connectionMultiplexer, db);
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

    private static async Task UpdateSingleCustomer(IDatabase db, int customerId, Customer updatedCustomer)
        {
            var redisKey = $"customer:{customerId}";
            var json = JsonConvert.SerializeObject(updatedCustomer);
            await db.ExecuteAsync("JSON.SET", redisKey, ".", json);
        }

        public static async Task UpdateCustomer(IConnectionMultiplexer _multiplexer, IDatabase db)
        {
            var customerId = 624633;
            var updatedCustomer = new Customer
            {
                Id = customerId,
                Name = "Updated Customer 9999",
                City = "Updated City 9999",
                Email = "updated_customer_9999@email.com"
            };
            await UpdateSingleCustomer(db, customerId, updatedCustomer);
        }
    }
}

