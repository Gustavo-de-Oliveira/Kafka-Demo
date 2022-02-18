using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Produtor
{
    class Program
    {
        public class Book {
            public int IdBook { get; set; }

            public string Author { get; set; }
            public string Genre { get; set; }
            public string Title { get; set; }
        
            public Book(int id, string author, string genre, string title)
            {
                IdBook = id;
                Author = author;
                Genre = genre;
                Title = title;
            }
        }

        public static async Task Main()
        {
            var config = new ProducerConfig { BootstrapServers = "127.0.0.1:9092" };

            using var p = new ProducerBuilder<Null, string>(config).Build();

            {
                try
                {
                    var count = 0;
                    while (true)
                    {
                        Book book = new Book(count, "Author " + count, "Genre " + count, "Title " + count);

                        var dr = await p.ProduceAsync("book-topic",
                            new Message<Null, string> { Value = $"IdBook: {book.IdBook}, Author: {book.Author}, Genre: {book.Genre}, Title: {book.Title};" });

                        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset} | {count}'");

                        count++;
                        Thread.Sleep(2000);
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}