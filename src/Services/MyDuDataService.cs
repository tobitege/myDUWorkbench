// Helper Index:
// - LoadConstructSnapshotAsync: Loads construct metadata, transforms, and decoded element properties from PostgreSQL.
// - GetUserConstructsAsync: Lists user-owned constructs by core type (dynamic/static/space) with configurable sorting.
// - SearchConstructsByNameAsync: Returns construct id/name suggestions via ILIKE matching.
// - ParseBlueprintJson: Flattens blueprint JSON into grid-friendly element property records.
// - ProbeEndpointAsync: Probes construct endpoint payloads and attempts JSON/binary decoding.
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
    private const long MaxBytesForInMemoryNqPreflight = 30L * 1024L * 1024L;
    private const long EstimatedDefaultJsonRequestBodyLimitBytes = 30_000_000L;
    private static readonly string[] DefaultCoreKindFilter = { "dynamic", "static", "space" };
    private static readonly string[] DefaultNqUtilsDllPaths =
    {
        @"D:\MyDUserver\wincs\all\NQutils.dll",
        @"d:\MyDUserver\wincs\all\NQutils.dll",
        @"D:\github\NQUtils\NQutils\bin\Debug\NQutils.dll",
        @"D:\github\NQUtils\NQutils\bin\Release\NQutils.dll"
    };

    private readonly HttpClient _httpClient;

    public MyDuDataService(HttpClient? httpClient = null)
    {
        _httpClient = httpClient ?? new HttpClient();
    }

}
