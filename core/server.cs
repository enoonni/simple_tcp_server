using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace SimpleTcpServer.Core;

public class Server
{
    private readonly int _port;
    private readonly string _logFilePath;

    private Socket? _listener;
    private CancellationTokenSource? _cts;
    private Task? _listenerTask;

    private readonly List<Task> _clientTasks = [];
    private readonly object _clientTasksLock = new();
    private readonly SemaphoreSlim _logLock = new(1, 1);

    public Server(int port, string logFilePath = "traffic.log")
    {
        _port = port;
        _logFilePath = logFilePath;
    }

    public void Start()
    {
        if(_listenerTask is not null)
            throw new InvalidOperationException("Server already started");

        _cts = new CancellationTokenSource();

        _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listener.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        _listener.Bind(new IPEndPoint(IPAddress.Any, _port));
        _listener.Listen(10);

        _listenerTask = Task.Run(() => ListenerLoopAsync(_cts.Token));

        Console.WriteLine($"Server started on port {_port}");
    }

    public async Task StopAsync()
    {
        if(_cts is null)
            return;

        _cts.Cancel();

        try
        {
            _listener?.Close();
        }
        catch
        {
        }

        if(_listenerTask is not null)
            await _listenerTask;

        Task[] clients;

        lock(_clientTasksLock)
        {
            clients = _clientTasks.ToArray();
        }

        await Task.WhenAll(clients);

        _cts.Dispose();
        _cts = null;
        _listenerTask = null;

        Console.WriteLine("Server stopped");
    }

    private async Task ListenerLoopAsync(CancellationToken token)
    {
        if(_listener is null)
            return;

        try
        {
            while(!token.IsCancellationRequested)
            {
                Socket client = await _listener.AcceptAsync(token);

                Task clientTask = Task.Run(() => HandleClientAsync(client, token), token);

                lock(_clientTasksLock)
                {
                    _clientTasks.Add(clientTask);
                }

                _ = clientTask.ContinueWith(task =>
                {
                    lock(_clientTasksLock)
                    {
                        _clientTasks.Remove(task);
                    }
                }, TaskScheduler.Default);
            }
        }
        catch(OperationCanceledException)
        {
        }
        catch(ObjectDisposedException)
        {
        }
        catch(Exception ex)
        {
            Console.WriteLine($"Listener error: {ex.Message}");
        }
    }

    private async Task HandleClientAsync(Socket client, CancellationToken token)
    {
        string remote = client.RemoteEndPoint?.ToString() ?? "unknown";

        Console.WriteLine($"Client connected: {remote}");

        byte[] buffer = new byte[4096];

        try
        {
            while(!token.IsCancellationRequested)
            {
                int bytesRead = await client.ReceiveAsync(buffer, SocketFlags.None, token);

                if(bytesRead == 0)
                    break;

                await WriteTrafficLogAsync(remote, buffer.AsMemory(0, bytesRead), token);
            }
        }
        catch(OperationCanceledException)
        {
        }
        catch(SocketException ex)
        {
            Console.WriteLine($"Client socket error {remote}: {ex.Message}");
        }
        catch(ObjectDisposedException)
        {
        }
        catch(Exception ex)
        {
            Console.WriteLine($"Client error {remote}: {ex.Message}");
        }
        finally
        {
            try
            {
                client.Shutdown(SocketShutdown.Both);
            }
            catch
            {
            }

            client.Close();

            Console.WriteLine($"Client disconnected: {remote}");
        }
    }

    private async Task WriteTrafficLogAsync(
        string remote,
        ReadOnlyMemory<byte> data,
        CancellationToken token)
    {
        string timestamp = DateTime.Now.ToString(
            "dd/MM/yyyy - HH:mm:ss:ffffff",
            CultureInfo.InvariantCulture);

        string hex = ConvertToHex(data.Span);

        string line = $"[{timestamp}]{{{remote}}} {hex}{Environment.NewLine}";

        await _logLock.WaitAsync(token);

        try
        {
            await File.AppendAllTextAsync(_logFilePath, line, token);
        }
        finally
        {
            _logLock.Release();
        }
    }

    private static string ConvertToHex(ReadOnlySpan<byte> data)
    {
        StringBuilder sb = new(data.Length * 3);

        foreach(byte b in data)
        {
            sb.Append(b.ToString("X2"));
            sb.Append(' ');
        }

        if(sb.Length > 0)
            sb.Length--;

        return sb.ToString();
    }
}