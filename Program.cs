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

           // await InsertCustomers(db, redisKey);
            await GetCustomers(db, redisKey);

            connectionMultiplexer.Close();
        }

        private static async Task InsertCustomers(IDatabase db, string redisKey)
        {
            var numCustomers = 1000000;
            var tasks = new List<Task>(numCustomers);

            await db.ExecuteAsync("JSON.SET", redisKey, ".", "{}");

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
                // tasks.Add(db.JsonSetAsync(redisKey, $".{i}", json));
                await db.ExecuteAsync("JSON.SET", redisKey, $".{i}", json);
            }

            await Task.WhenAll(tasks);

            stopwatch.Stop();

            Console.WriteLine($"Inserted 1000000 customers in {stopwatch.ElapsedMilliseconds} ms.");
        }

        //Get all customers
        private static async Task GetCustomers(IDatabase db, string redisKey)
        {
             var stopwatch = new Stopwatch();
             stopwatch.Start();

            var redisValue = await db.ExecuteAsync("JSON.GET", redisKey);
            if (!redisValue.IsNull)
            {
                // Get the string representation of the RedisResult
                string json = redisValue.ToString();

                // Deserialize the JSON string into a Customer object
                var retrievedCustomer = JsonConvert.DeserializeObject<Customer>(json);
                stopwatch.Stop();

                Console.WriteLine($"Retrieved 1000000 customers in {stopwatch.ElapsedMilliseconds} ms.");
            }
        }
    }
}

