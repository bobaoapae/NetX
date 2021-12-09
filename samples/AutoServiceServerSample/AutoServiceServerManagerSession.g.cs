﻿using AutoServiceServerSample.Definitions;
using Microsoft.IO;
using NetX;
using System;
using System.Net;
using AutoServiceClientSample.Definitions;

namespace AutoServiceServerSample;

public class AutoServiceServerManagerSession : NetXSession
{
    private RecyclableMemoryStreamManager _streamManager;
    public IAutoServiceClientSample AutoServiceSample { get; }

    public AutoServiceServerManagerSession(Guid guid, IPAddress ipAddress, RecyclableMemoryStreamManager streamManager)
    {
        _streamManager = streamManager;

        #region InitializeServices

        AutoServiceSample = new AutoServiceClientSampleConsumer(this, streamManager);

        #endregion
    }
    
}