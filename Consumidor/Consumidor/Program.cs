using Confluent.Kafka;
using System;
using System.Threading;

namespace Consumidor
{
    class Program
    {
        public class Book
        {
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

        public static void Main(string[] args)
        {
            string getValor(string retorno, string key)
            {
                string valeu = "";
                int i = retorno.LastIndexOf(key + ": ");
                i += key.Length + 2;
                while (i <= retorno.Length)
                {
                    char ch = retorno[i];
                    if (ch == ',' || ch == ';')
                        break;
                    valeu += ch;
                    i++;
                }
                return valeu;
            }

            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "127.0.0.1:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var c = new ConsumerBuilder<Ignore, string>(conf).Build();

            {
                c.Subscribe("book-topic");

                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            string str = cr.Value.ToString();

                            Console.WriteLine($"Consumed message '{str}' at: '{cr.TopicPartitionOffset}'.");

                            string chr = getValor(str, "IdBook").ToString();

                            int i = Convert.ToInt32(chr.ToString().Trim());
                            Book book = new Book(i, getValor(str, "Author"), getValor(str, "Genre"), getValor(str, "Title"));

                            Console.WriteLine($"Obj = {{\n    IdBook = {book.IdBook};\n    Author = {book.Author};\n    Genre = {book.Genre};\n    Title = {book.Title};\n }}\n");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }
        }
    }
}