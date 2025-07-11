using Microsoft.Data.Sqlite;
using System.Diagnostics;
using System.IO.Compression;
using System.Text;

namespace imdb;

public class ImdbGzipDataImporter(string ratingsDbPath, string imdbDataDirPath)
{
    private readonly string _ratingsDbPath = ratingsDbPath;
    private readonly string _imdbDataDirPath = imdbDataDirPath;

    private readonly ImdbXconstSet _ratedTconsts = new();
    private readonly ImdbXconstSet _relevantNconsts = new();

    public void ImportAllRelevantImdbData()
    {
        Console.WriteLine($"Starting IMDb data import to: {_ratingsDbPath}");
        Console.WriteLine($"Looking for TSV.GZ files in: {_imdbDataDirPath}");

        try
        {
            Stopwatch totalSw = Stopwatch.StartNew();

            // Step 1: Pre-load rated tconsts from the ratings database
            LoadRatedTconsts();

            // Step 2: Process title.akas.tsv.gz
            ProcessTitleAkas();
            // Step 3: Process title.principals.tsv.gz and build relevantNconsts
            ProcessTitlePrincipalsAndBuildNconsts();
            // Step 4: Process name.basics.tsv.gz
            ProcessNameBasics();

            /*var akasTask = Task.Run(() => ProcessTitleAkas());
            var principalsTask = Task.Run(() => ProcessTitlePrincipalsAndBuildNconsts());
            var nameBasicsContinuationTask = principalsTask.ContinueWith(_ => ProcessNameBasics(), TaskScheduler.Default);
            Task.WhenAll(akasTask, nameBasicsContinuationTask).Wait();*/

            totalSw.Stop();
            Console.WriteLine($"\n--- All relevant IMDb data imported in {totalSw.Elapsed.TotalSeconds:F2} seconds ---");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"\nAn unhandled error occurred during import: {ex.Message}");
            Console.Error.WriteLine(ex.StackTrace);
        }
    }

    private void LoadRatedTconsts()
    {
        Console.WriteLine("\nLoading rated tconsts from 'ratings' table...");
        Stopwatch sw = Stopwatch.StartNew();

        using (var connection = new SqliteConnection($"Data Source={_ratingsDbPath}"))
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT DISTINCT const FROM ratings;";
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                _ratedTconsts.Add(reader.GetString(0));
            }
        }

        sw.Stop();
        Console.WriteLine($"Loaded {_ratedTconsts.Count} unique rated tconsts in {sw.ElapsedMilliseconds} ms.");
        if (_ratedTconsts.Count == 0)
        {
            Console.WriteLine("Warning: No rated tconsts found. No IMDb data will be imported.");
        }
    }

    SqliteConnection CreateConnectionForWriting(string tableName)
    {
        var connection = new SqliteConnection($"Data Source={_ratingsDbPath}");
        try
        {
            connection.Open();

            using (var pragmaCmd = connection.CreateCommand())
            {
                pragmaCmd.CommandText = "PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF";
                pragmaCmd.ExecuteNonQuery();
            }

            // Clear existing data in the target table (optional, but good for re-runs)
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $"DELETE FROM {tableName}";
                cmd.ExecuteNonQuery();
                Console.WriteLine($"Cleared existing data in '{tableName}'.");
            }

            return connection;
        }
        catch 
        {
            connection?.Dispose();
            throw;
        }
    }

    private void ProcessTitleAkas()
    {
        string filePath = Path.Combine(_imdbDataDirPath, "title.akas.tsv.gz");
        const string tableName = "title_akas";

        Console.WriteLine($"\nProcessing {Path.GetFileName(filePath)} for table '{tableName}'...");
        Stopwatch sw = Stopwatch.StartNew();

        using var connection = CreateConnectionForWriting(tableName);

        // Prepare the insert command parameters once
        using var insertCommand = connection.CreateCommand();
        insertCommand.CommandText = @"
            INSERT INTO title_akas (titleId, ordering, title, region, language, types, attributes, isOriginalTitle)
            VALUES (@titleId, @ordering, @title, @region, @language, @types, @attributes, @isOriginalTitle)";
        insertCommand.Parameters.Add("@titleId", SqliteType.Text);
        insertCommand.Parameters.Add("@ordering", SqliteType.Integer);
        insertCommand.Parameters.Add("@title", SqliteType.Text);
        insertCommand.Parameters.Add("@region", SqliteType.Text);
        insertCommand.Parameters.Add("@language", SqliteType.Text);
        insertCommand.Parameters.Add("@types", SqliteType.Text);
        insertCommand.Parameters.Add("@attributes", SqliteType.Text);
        insertCommand.Parameters.Add("@isOriginalTitle", SqliteType.Integer);

        using var transaction = connection.BeginTransaction();
        insertCommand.Transaction = transaction;

        using var reader = new ImdbGzipReader(filePath, _ratedTconsts);
        string[]? parts;
        while ((parts = reader.GetNextIncludedRow()) != null)
        {
            insertCommand.Parameters["@titleId"].Value = parts[0];
            insertCommand.Parameters["@ordering"].Value = int.TryParse(parts[1], out int orderingVal) ? orderingVal : DBNull.Value;
            insertCommand.Parameters["@title"].Value = parts[2];
            insertCommand.Parameters["@region"].Value = parts[3];
            insertCommand.Parameters["@language"].Value = parts[4];
            insertCommand.Parameters["@types"].Value = parts[5];
            insertCommand.Parameters["@attributes"].Value = parts[6];
            insertCommand.Parameters["@isOriginalTitle"].Value = (parts[7] == "1" ? 1 : 0);
            insertCommand.ExecuteNonQuery();
        }
        transaction.Commit();

        sw.Stop();
        Console.WriteLine($"Finished {Path.GetFileName(filePath)}. Total lines read: {reader.LinesRead}. Total Akas entries inserted: {reader.EntriesReturned} in {sw.Elapsed.TotalSeconds:F2} seconds.");
    }

    private void ProcessTitlePrincipalsAndBuildNconsts()
    {
        string filePath = Path.Combine(_imdbDataDirPath, "title.principals.tsv.gz");
        string tableName = "title_principals";
        Console.WriteLine($"\nProcessing {Path.GetFileName(filePath)} for table '{tableName}' and building relevant NCONSTs...");
        Stopwatch sw = Stopwatch.StartNew();

        using var connection = CreateConnectionForWriting(tableName);

        using var insertCommand = connection.CreateCommand();
        insertCommand.CommandText = @"
            INSERT INTO title_principals (tconst, ordering, nconst, category, job, characters)
            VALUES (@tconst, @ordering, @nconst, @category, @job, @characters)";
        insertCommand.Parameters.Add("@tconst", SqliteType.Text);
        insertCommand.Parameters.Add("@ordering", SqliteType.Integer);
        insertCommand.Parameters.Add("@nconst", SqliteType.Text);
        insertCommand.Parameters.Add("@category", SqliteType.Text);
        insertCommand.Parameters.Add("@job", SqliteType.Text);
        insertCommand.Parameters.Add("@characters", SqliteType.Text);

        using var reader = new ImdbGzipReader(filePath, _ratedTconsts);

        using var transaction = connection.BeginTransaction();
        insertCommand.Transaction = transaction;

        string[]? parts; 
        while ((parts = reader.GetNextIncludedRow()) != null)
        {
            _relevantNconsts.Add(parts[2]);
            insertCommand.Parameters["@tconst"].Value = parts[0];
            insertCommand.Parameters["@ordering"].Value = int.TryParse(parts[1], out int orderingVal) ? orderingVal : DBNull.Value;
            insertCommand.Parameters["@nconst"].Value = parts[2];
            insertCommand.Parameters["@category"].Value = parts[3];
            insertCommand.Parameters["@job"].Value = parts[4];
            insertCommand.Parameters["@characters"].Value = parts[5];
            insertCommand.ExecuteNonQuery();
        }
        transaction.Commit();

        sw.Stop();
        Console.WriteLine($"Finished {Path.GetFileName(filePath)}. Total lines read: {reader.LinesRead}. Total Principals entries inserted: {reader.EntriesReturned} in {sw.Elapsed.TotalSeconds:F2} seconds.");
        Console.WriteLine($"Unique relevant NCONSTs collected: {_relevantNconsts.Count}");
        
    }

    private void ProcessNameBasics()
    {
        string filePath = Path.Combine(_imdbDataDirPath, "name.basics.tsv.gz");
        string tableName = "name_basics";
        Console.WriteLine($"\nProcessing {Path.GetFileName(filePath)} for table '{tableName}'...");
        Stopwatch sw = Stopwatch.StartNew();

        using var connection = CreateConnectionForWriting(tableName);

        using var insertCommand = connection.CreateCommand();
        insertCommand.CommandText = @"
            INSERT INTO name_basics (nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles)
            VALUES (@nconst, @primaryName, @birthYear, @deathYear, @primaryProfession, @knownForTitles)";
        insertCommand.Parameters.Add("@nconst", SqliteType.Text);
        insertCommand.Parameters.Add("@primaryName", SqliteType.Text);
        insertCommand.Parameters.Add("@birthYear", SqliteType.Integer);
        insertCommand.Parameters.Add("@deathYear", SqliteType.Integer);
        insertCommand.Parameters.Add("@primaryProfession", SqliteType.Text);
        insertCommand.Parameters.Add("@knownForTitles", SqliteType.Text);

        using var reader = new ImdbGzipReader(filePath, _relevantNconsts);

        using var transaction = connection.BeginTransaction();
        insertCommand.Transaction = transaction;

        string[]? parts;
        while ((parts = reader.GetNextIncludedRow()) != null)
        {
            insertCommand.Parameters["@nconst"].Value = parts[0];
            insertCommand.Parameters["@primaryName"].Value = parts[1];
            insertCommand.Parameters["@birthYear"].Value = int.TryParse(parts[2], out int birthYearVal) ? birthYearVal : DBNull.Value;
            insertCommand.Parameters["@deathYear"].Value = int.TryParse(parts[3], out int deathYearVal) ? deathYearVal : DBNull.Value;
            insertCommand.Parameters["@primaryProfession"].Value = parts[4];
            insertCommand.Parameters["@knownForTitles"].Value = parts[5];
            insertCommand.ExecuteNonQuery();
        }
        transaction.Commit();

        sw.Stop();
        Console.WriteLine($"Finished {Path.GetFileName(filePath)}. Total lines read: {reader.LinesRead}. Total Name Basics entries inserted: {reader.EntriesReturned} in {sw.Elapsed.TotalSeconds:F2} seconds.");
    }
}

public class Program
{
    public static void Main(string[] args)
    {
        string ratingsDatabasePath = "D:\\Downloads\\imdb\\gz\\ratings.sqlite";
        string imdbFilesDirectory = "D:\\Downloads\\imdb\\gz";

        var importer = new ImdbGzipDataImporter(ratingsDatabasePath, imdbFilesDirectory);
        importer.ImportAllRelevantImdbData();
    }
}

public class ImdbXconstSet
{
    private readonly HashSet<long> _ids = [];

    public void Add(string Xconst)
    {
        var id = GetId(Xconst);
        _ids.Add(id);
    }

    public bool Contains(ReadOnlySpan<byte> Xconst)
    {
        var id = GetId(Xconst);
        return _ids.Contains(id);
    }

    public int Count => _ids.Count;

    public static long GetId(string XconstString)
    {
        var Xconst = XconstString.AsSpan();
        long id = 0;
        for (int i = 2; i < Xconst.Length; ++i)
        {
            id = id * 10 + (Xconst[i] - '0');
        }
        return id;
    }

    public static long GetId(ReadOnlySpan<byte> Xconst)
    {
        long id = 0;
        for (int i = 2; i < Xconst.Length ; ++i)
        {
            id = id * 10 + (Xconst[i] - (byte)'0');
        }
        return id;
    }
}


public class ImdbGzipReader : IDisposable
{
    private readonly FileStream _fileStream;
    private readonly GZipStream _gzipStream;
    private readonly byte[] _buffer;
    private readonly ImdbXconstSet _includedXconsts;

    private int _bytesInBuffer;  // Number of valid bytes currently in the buffer
    private int _readOffset;     // Where the next search for a line starts in the buffer

    private int _linesRead;
    private int _entriesReturned;

    public ImdbGzipReader(string filePath, ImdbXconstSet includedXconsts, int bufferSize = 128 * 1024)
    {
        _fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, FileOptions.SequentialScan);
        _gzipStream = new GZipStream(_fileStream, CompressionMode.Decompress);
        _buffer = new byte[bufferSize];
        _includedXconsts = includedXconsts;
                
        ReadNextLine(); // Skip past the header line.
    }

    public void Dispose()
    {
        _gzipStream?.Dispose();
        _fileStream?.Dispose();
        GC.SuppressFinalize(this);
    }

    public string[]? GetNextIncludedRow()
    {
        while (true)
        {
            var line = ReadNextLine();
            if (line.Length == 0)
            {
                // !!! Verify that all data has been read
                return null;
            }

            int firstTabIndex = line.IndexOf((byte)'\t');
            if (firstTabIndex == -1)
            {
                throw new InvalidDataException("Could not extra Xconst field");
            }

            var Xconst = line.Slice(0, firstTabIndex);
            if (_includedXconsts.Contains(Xconst))
            {
                var lineStr = Encoding.UTF8.GetString(line);
                ++_entriesReturned;
                return lineStr.Split('\t');
            }
        }
    }

    public long LinesRead => _linesRead;

    public long EntriesReturned => _entriesReturned;
        
    public ReadOnlySpan<byte> ReadNextLine()
    {
        while (true)
        {
            // 1. Search for a newline in the current buffer segment
            var currentView = _buffer.AsSpan(_readOffset, _bytesInBuffer - _readOffset);
            int newlineIndex = currentView.IndexOf((byte)'\n');

            if (newlineIndex != -1)
            {
                var line = _buffer.AsSpan(_readOffset, newlineIndex);
                _readOffset += (newlineIndex + 1); // Move past the consumed line (including \n)
                ++_linesRead;
                return line;
            }
                
            // 2. If no newline found, try to refill the buffer.
            // First, shift any remaining data to the beginning
            if (_readOffset > 0)
            { 
                int bytesRemaining = _bytesInBuffer - _readOffset;
                if (bytesRemaining > 0)
                {
                    _buffer.AsSpan(_readOffset, bytesRemaining).CopyTo(_buffer.AsSpan(0));
                }
                _bytesInBuffer = bytesRemaining;
                _readOffset = 0;
            }

            // Then, read more data to fill the buffer
            int bytesRead = _gzipStream.Read(_buffer, _bytesInBuffer, _buffer.Length - _bytesInBuffer);

            if (bytesRead == 0) // End of stream reached, no more data to read
            {
                if (_bytesInBuffer > 0)
                {
                    throw new InvalidDataException("Line too long or did not end with newline");
                }
                return default; // Truly end of stream
            }
            _bytesInBuffer += bytesRead; // Update total valid bytes
        }
    }
}