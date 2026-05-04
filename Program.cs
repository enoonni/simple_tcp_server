using SimpleTcpServer.Core;

var server = new Server(31337);
server.Start();

while (true)
{
    var input = Console.ReadLine();

    switch (input)
    {
        case "0":
            server.StopAsync().Wait();
            return;
        default:
            break;
    }
}