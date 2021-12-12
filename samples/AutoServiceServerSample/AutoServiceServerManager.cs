﻿using System;
using System.Threading.Tasks;
using AutoServiceClientSample.Definitions;
using NetX.AutoServiceGenerator.Definitions;

namespace AutoServiceServerSample;

[AutoServiceProvider(typeof(AutoServiceSample))]
[AutoServiceConsumer(typeof(IAutoServiceReceiverSample))]
public partial class AutoServiceServerManager : IAutoServiceServerManager
{
    public Task OnSessionConnectAsync(AutoServiceServerManagerSession session)
    {
        Console.WriteLine($"Session Connected: {session.Session.Id}");
        return Task.CompletedTask;
    }

    public Task OnSessionDisconnectAsync(AutoServiceServerManagerSession session)
    {
        Console.WriteLine($"Session Disconnected: {session.Session.Id}");
        return Task.CompletedTask;
    }
}