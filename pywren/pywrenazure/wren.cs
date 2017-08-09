using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.NetworkInformation;


public static string ConvertBase64(string s)
{
    return BitConverter.ToString(Convert.FromBase64String(s));
}

public static string[] ServerInfo()
{
    return null;
}

class PywrenArgs
{
    public Dictionary<string, string> storage_config;
    public string func_key;
    public string data_key;
    public string output_key;
    public string status_key;
    public string callset_id;
    public int job_max_runtime;
    public int[] data_byte_range;
    public string pywren_version;
    public bool use_cached_runtime;
    public string call_id;
    public OSMETHING runtime;
    public string runtime_url;
}

class PywrenResponse
{
    public string exception;
    public double start_time;
    public string func_key;
    public string data_key;
    public string output_key;
    public string status_key;
    public string call_id;
    public string callset_id;
    public double setup_time;
    public string stdout;
    public double exec_time;
    public Dictionary<string, string>server_info;
    public Dictionary<string, string>exception_args;
    public string exception_traceback;
}

public static void Run(string queueMessage, string funcfile, string datafile,
    out string outputfile, out string statusfile,
    TraceWriter log)
{
    string TEMP = @"D:\local\Temp";
    string PYTHON_MODULE_PATH = System.IO.Path.Combine(TEMP, "modules");
    string CONDA_PYTHON_PATH = @"D:\home\site\wwwroot\conda\Miniconda2";
    string PYTHON = System.IO.Path.Combine(CONDA_PYTHON_PATH, "python");

    Dictionary<string, string> response_status =
        new Dictionary<string, string>();

    var args =
        JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(queueMessage);

    response_status.Add("exception", null);

    try {
        DateTime start_time = DateTime.UtcNow; 
        string func_file_location = System.IO.Path.Combine(TEMP, args["callset_id"] + args["call_id"] + "func");
        string data_file_location = System.IO.Path.Combine(TEMP, args["callset_id"] + args["call_id"] + "data");
        string data_byte_range = args["data_byte_range"];
        int job_max_runtime;
        if (args.TryGetValue("job_max_runtime", out value)) {
            job_max_runtime = Convert.ToInt32(value);
        } else {
            job_max_runtime = 600;
        }


        Process p = new Process();
        p.StartInfo.FileName = PYTHON;
        p.StartInfo.Arguments = "D:\\home\\site\\wwwroot\\ohohohoho\\run.py";
        p.Start();
        p.WaitForExit();

    } catch(Exception ex) {
        response_status["exception"] = ex.Message; 
        response_status["exception_traceback"] = ex.StackTrace;
    } finally {
        response_status["server_info"] = get_server_info();
        statusfile = DictToJSON(response_status);
    }
}

