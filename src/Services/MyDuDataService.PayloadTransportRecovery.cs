using myDUWorkbench.Models;
using Newtonsoft.Json;
using Npgsql;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorkbench.Services;

public sealed partial class MyDuDataService
{
    private static string BuildSingleLineExceptionPreview(Exception ex)
    {
        string message = ex.Message ?? string.Empty;
        message = message.Replace("\r", " ").Replace("\n", " ").Trim();
        if (message.Length <= 220)
        {
            return message;
        }

        return message[..217] + "...";
    }

    private static bool IsConnectionResetException(HttpRequestException ex)
    {
        for (Exception? current = ex; current is not null; current = current.InnerException)
        {
            if (current is SocketException socketException)
            {
                if (socketException.SocketErrorCode == SocketError.ConnectionReset ||
                    socketException.SocketErrorCode == SocketError.ConnectionAborted)
                {
                    return true;
                }
            }

            if (current is IOException ioException &&
                ioException.Message.Contains("closed", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsConnectionRefusedException(HttpRequestException ex)
    {
        for (Exception? current = ex; current is not null; current = current.InnerException)
        {
            if (current is SocketException socketException &&
                socketException.SocketErrorCode == SocketError.ConnectionRefused)
            {
                return true;
            }
        }

        return false;
    }

    private static bool ShouldAttemptTransportRecovery(HttpRequestException ex)
    {
        return IsConnectionResetException(ex) || IsConnectionRefusedException(ex);
    }

    private static async Task WaitForEndpointPortRecoveryAsync(
        Uri endpoint,
        TimeSpan maxWait,
        CancellationToken cancellationToken)
    {
        DateTime startedAt = DateTime.UtcNow;
        while (DateTime.UtcNow - startedAt < maxWait)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await CanConnectTcpAsync(endpoint.Host, endpoint.Port, cancellationToken))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        }
    }

    private static async Task<bool> CanConnectTcpAsync(string host, int port, CancellationToken cancellationToken)
    {
        try
        {
            using var client = new TcpClient();
            Task connectTask = client.ConnectAsync(host, port);
            Task completed = await Task.WhenAny(connectTask, Task.Delay(TimeSpan.FromSeconds(1), cancellationToken));
            if (!ReferenceEquals(completed, connectTask))
            {
                return false;
            }

            await connectTask;
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static string BuildTransportErrorPreview(HttpRequestException ex)
    {
        string message = ex.Message;
        if (ex.InnerException is SocketException socketException)
        {
            return $"{message} (socket {(int)socketException.SocketErrorCode}: {socketException.SocketErrorCode})";
        }

        return message;
    }
}
