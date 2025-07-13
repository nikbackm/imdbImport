using Microsoft.Data.Sqlite;
using System.Diagnostics;
using System.IO.Compression;
using System.Text;

namespace imdbImport;

public class Program
{
    public static int Main(string[] args)
    {
        if (args.Length != 2)
        {
            Console.WriteLine("Usage: <ratingsDbPath> <imdbDataDirPath>");
            return 1;
        }

        var ratingsDbPath = args[0];
        var imdbDataDirPath = args[1];

        if (!File.Exists(ratingsDbPath))
        {
            Console.Error.WriteLine($"Cannot find '{ratingsDbPath}'");
            return 1;
        }

        if (!Directory.Exists(imdbDataDirPath))
        {
            Console.Error.WriteLine($"Cannot find '{imdbDataDirPath}'");
            return 1;
        }

        var importer = new ImdbGzipDataImporter(ratingsDbPath, imdbDataDirPath);
        return importer.ImportAllRelevantImdbData();
    }
}

public class ImdbGzipDataImporter(string ratingsDbPath, string imdbDataDirPath)
{
    private readonly string _ratingsDbPath = ratingsDbPath;
    private readonly string _imdbDataDirPath = imdbDataDirPath;

    private readonly XconstSet _ratedTconsts = new();
    private readonly XconstSet _relevantNconsts = new();

    private readonly List<string[]> _titleBasics = [];
    private readonly List<string[]> _titleAkas = [];
    private readonly List<string[]> _titlePrincipals = [];
    private readonly List<string[]> _nameBasics = [];

    public int ImportAllRelevantImdbData()
    {
        Console.WriteLine($"Starting IMDb data import to: {_ratingsDbPath}");
        Console.WriteLine($"Looking for TSV.GZ files in: {_imdbDataDirPath}");

        try
        {
            Stopwatch totalSw = Stopwatch.StartNew();

            LoadRatedTconsts();

            var tasks = new List<Task>
            {
                Task.Run(() => { ProcessTitlePrincipalsAndBuildNconsts(); ProcessNameBasics(); }),
                Task.Run(() => ProcessTitleBasics()),
                Task.Run(() => ProcessTitleAkas())
            };
            Task.WaitAll(tasks);

            AddToDatabase();

            totalSw.Stop();
            Console.WriteLine($"\n--- All relevant IMDb data imported in {totalSw.Elapsed.TotalSeconds:F2} seconds ---");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"\nAn unhandled error occurred during import: {ex.Message}");
            Console.Error.WriteLine(ex.StackTrace);
            return 2;
        }
    }

    private void LoadRatedTconsts()
    {
        Console.WriteLine("\nLoading rated tconsts from ratings ...");
        Stopwatch sw = Stopwatch.StartNew();

        using (var connection = new SqliteConnection($"Data Source={_ratingsDbPath};Mode=ReadOnly"))
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

    private void ProcessTitleBasics()
    {
        string filePath = Path.Combine(_imdbDataDirPath, "title.basics.tsv.gz");
        Console.WriteLine($"\nProcessing {Path.GetFileName(filePath)}...");
        Stopwatch sw = Stopwatch.StartNew();

        using var reader = new GzipReader(filePath, _ratedTconsts);
        string[]? parts;
        while ((parts = reader.GetNextIncludedRow()) != null)
        {
            _titleBasics.Add(parts);
        }

        sw.Stop();
        Console.WriteLine($"Finished {Path.GetFileName(filePath)}. Total lines read: {reader.LinesRead}. Total Titles entries retrieved: {reader.EntriesReturned} in {sw.Elapsed.TotalSeconds:F2} seconds.");
    }

    private void ProcessTitleAkas()
    {
        string filePath = Path.Combine(_imdbDataDirPath, "title.akas.tsv.gz");
        Console.WriteLine($"\nProcessing {Path.GetFileName(filePath)}...");
        Stopwatch sw = Stopwatch.StartNew();

        using var reader = new GzipReader(filePath, _ratedTconsts);
        string[]? parts;
        while ((parts = reader.GetNextIncludedRow()) != null)
        {
            var region = parts[3];
            if (region == "\\N" || region == "SE" || region == "FI" || region == "US" && parts[4] == "\\N")
            {
                _titleAkas.Add(parts);
            }
        }

        sw.Stop();
        Console.WriteLine($"Finished {Path.GetFileName(filePath)}. Total lines read: {reader.LinesRead}. Total Akas entries retrieved: {reader.EntriesReturned} in {sw.Elapsed.TotalSeconds:F2} seconds.");
    }

    private void ProcessTitlePrincipalsAndBuildNconsts()
    {
        string filePath = Path.Combine(_imdbDataDirPath, "title.principals.tsv.gz");
        Console.WriteLine($"\nProcessing {Path.GetFileName(filePath)} and building relevant NCONSTs...");
        Stopwatch sw = Stopwatch.StartNew();

        using var reader = new GzipReader(filePath, _ratedTconsts);
        string[]? parts;
        while ((parts = reader.GetNextIncludedRow()) != null)
        {
            _relevantNconsts.Add(parts[2]);
            _titlePrincipals.Add(parts);
        }

        sw.Stop();
        Console.WriteLine($"Finished {Path.GetFileName(filePath)}. Total lines read: {reader.LinesRead}. Total Principals entries retrieved: {reader.EntriesReturned} in {sw.Elapsed.TotalSeconds:F2} seconds.");
        Console.WriteLine($"Unique relevant NCONSTs collected: {_relevantNconsts.Count}");
    }

    private void ProcessNameBasics()
    {
        string filePath = Path.Combine(_imdbDataDirPath, "name.basics.tsv.gz");
        Console.WriteLine($"\nProcessing {Path.GetFileName(filePath)}...");
        Stopwatch sw = Stopwatch.StartNew();

        using var reader = new GzipReader(filePath, _relevantNconsts);
        string[]? parts;
        while ((parts = reader.GetNextIncludedRow()) != null)
        {
            _nameBasics.Add(parts);
        }

        sw.Stop();
        Console.WriteLine($"Finished {Path.GetFileName(filePath)}. Total lines read: {reader.LinesRead}. Total Name Basics entries retrieved: {reader.EntriesReturned} in {sw.Elapsed.TotalSeconds:F2} seconds.");
    }

    /*
     * 
*/
   

    const string createTablesSql = @"
        CREATE TABLE name_basics (
            nconst TEXT PRIMARY KEY NOT NULL, 
            primaryName TEXT NOT NULL,        -- The person's main name
            birthYear INTEGER,
            deathYear INTEGER,
            primaryProfession TEXT,           -- E.g., ""actor,producer,director""
            knownForTitles TEXT               -- List of tconsts they are known for (comma-separated, typically for top 4)
        ) WITHOUT ROWID;

        CREATE TABLE title_principals (
            tconst TEXT NOT NULL,           
            ordering INTEGER NOT NULL,      -- Order of the credit
            nconst TEXT NOT NULL,           -- Matches 'nconst' from name.basics.tsv (person ID)
            category TEXT NOT NULL,         -- E.g., ""actor"", ""actress"", ""director"", ""writer""
            job TEXT,                       -- Specific job within category (e.g., ""cinematographer"")
            characters TEXT,                -- Character names played by the actor (JSON array for multiple)
            PRIMARY KEY (tconst, ordering)
        ) WITHOUT ROWID;

        CREATE TABLE title_basics (
            tconst TEXT PRIMARY KEY NOT NULL, 
            titleType TEXT,                -- the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
            primaryTitle TEXT,             -- the more popular title / the title used by the filmmakers on promotional materials at the point of release
            originalTitle TEXT,            -- original title, in the original language
            isAdult INTEGER,               -- 0: non-adult title; 1: adult title
            startYear INTEGER,             -- represents the release year of a title. In the case of TV Series, it is the series start year
            endYear INTEGER,               -- TV Series end year. '\N' for all other title types
            runtimeMinutes INTEGER,        -- primary runtime of the title, in minutes
            genres TEXT                    -- includes up to three genres associated with the title
        ) WITHOUT ROWID;

        CREATE TABLE title_akas (
            tconst TEXT NOT NULL,        
            ordering INTEGER NOT NULL,   -- Order of the AKA (from title.akas.tsv)
            title TEXT NOT NULL,         -- The alternate title itself
            region TEXT,                 -- Region where this AKA is used (e.g., ""US"", ""GB"")
            language TEXT,               -- Language of the AKA (e.g., ""en"", ""fr"")
            types TEXT,                  -- Type of AKA (e.g., ""alternative"", ""dvd"", ""working"")
            attributes TEXT,             -- Additional attributes (e.g., ""original title"")
            isOriginalTitle INTEGER,     -- '0' or '1' from title.akas.tsv (BOOLEAN in spirit)
            PRIMARY KEY (tconst, ordering)
        ) WITHOUT ROWID;";

    private void AddToDatabase()
    {
        Console.WriteLine($"\nAdding to database...");
        Stopwatch sw = Stopwatch.StartNew();

        using var connection = new SqliteConnection($"Data Source={_ratingsDbPath}");
        connection.Open();

        using (var pragmaCmd = connection.CreateCommand())
        {
            pragmaCmd.CommandText = "PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF";
            pragmaCmd.ExecuteNonQuery();
        }

        using var createTables = connection.CreateCommand();
        createTables.CommandText = createTablesSql;

        using var insertTitleBasics = connection.CreateCommand();
        insertTitleBasics.CommandText = @"
            INSERT INTO title_basics (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
            VALUES (@tconst, @titleType, @primaryTitle, @originalTitle, @isAdult, @startYear, @endYear, @runtimeMinutes, @genres)";
        insertTitleBasics.Parameters.Add("@tconst", SqliteType.Text);
        insertTitleBasics.Parameters.Add("@titleType", SqliteType.Text);
        insertTitleBasics.Parameters.Add("@primaryTitle", SqliteType.Text);
        insertTitleBasics.Parameters.Add("@originalTitle", SqliteType.Text);
        insertTitleBasics.Parameters.Add("@isAdult", SqliteType.Integer);
        insertTitleBasics.Parameters.Add("@startYear", SqliteType.Integer);
        insertTitleBasics.Parameters.Add("@endYear", SqliteType.Integer);
        insertTitleBasics.Parameters.Add("@runtimeMinutes", SqliteType.Integer);
        insertTitleBasics.Parameters.Add("@genres", SqliteType.Text);

        using var insertTitleAkas = connection.CreateCommand();
        insertTitleAkas.CommandText = @"
            INSERT INTO title_akas (tconst, ordering, title, region, language, types, attributes, isOriginalTitle)
            VALUES (@tconst, @ordering, @title, @region, @language, @types, @attributes, @isOriginalTitle)";
        insertTitleAkas.Parameters.Add("@tconst", SqliteType.Text);
        insertTitleAkas.Parameters.Add("@ordering", SqliteType.Integer);
        insertTitleAkas.Parameters.Add("@title", SqliteType.Text);
        insertTitleAkas.Parameters.Add("@region", SqliteType.Text);
        insertTitleAkas.Parameters.Add("@language", SqliteType.Text);
        insertTitleAkas.Parameters.Add("@types", SqliteType.Text);
        insertTitleAkas.Parameters.Add("@attributes", SqliteType.Text);
        insertTitleAkas.Parameters.Add("@isOriginalTitle", SqliteType.Integer);

        using var insertTitlePrincipals = connection.CreateCommand();
        insertTitlePrincipals.CommandText = @"
            INSERT INTO title_principals (tconst, ordering, nconst, category, job, characters)
            VALUES (@tconst, @ordering, @nconst, @category, @job, @characters)";
        insertTitlePrincipals.Parameters.Add("@tconst", SqliteType.Text);
        insertTitlePrincipals.Parameters.Add("@ordering", SqliteType.Integer);
        insertTitlePrincipals.Parameters.Add("@nconst", SqliteType.Text);
        insertTitlePrincipals.Parameters.Add("@category", SqliteType.Text);
        insertTitlePrincipals.Parameters.Add("@job", SqliteType.Text);
        insertTitlePrincipals.Parameters.Add("@characters", SqliteType.Text);

        using var insertNameBasics = connection.CreateCommand();
        insertNameBasics.CommandText = @"
            INSERT INTO name_basics (nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles)
            VALUES (@nconst, @primaryName, @birthYear, @deathYear, @primaryProfession, @knownForTitles)";
        insertNameBasics.Parameters.Add("@nconst", SqliteType.Text);
        insertNameBasics.Parameters.Add("@primaryName", SqliteType.Text);
        insertNameBasics.Parameters.Add("@birthYear", SqliteType.Integer);
        insertNameBasics.Parameters.Add("@deathYear", SqliteType.Integer);
        insertNameBasics.Parameters.Add("@primaryProfession", SqliteType.Text);
        insertNameBasics.Parameters.Add("@knownForTitles", SqliteType.Text);

        static object TranslateNulls(string s) => s == "\\N" ? DBNull.Value : s;

        static object ParseInt(string s) => s == "\\N" ? DBNull.Value : int.Parse(s);

        using (var transaction = connection.BeginTransaction())
        {
            createTables.Transaction = transaction;
            insertTitleBasics.Transaction = transaction;
            insertTitleAkas.Transaction = transaction;
            insertTitlePrincipals.Transaction = transaction;
            insertNameBasics.Transaction = transaction;

            createTables.ExecuteNonQuery();

            foreach (var parts in _titleBasics)
            {
                insertTitleBasics.Parameters["@tconst"].Value = parts[0];
                insertTitleBasics.Parameters["@titleType"].Value = parts[1];
                insertTitleBasics.Parameters["@primaryTitle"].Value = parts[2];
                insertTitleBasics.Parameters["@originalTitle"].Value = parts[3];
                insertTitleBasics.Parameters["@isAdult"].Value = parts[4] == "1" ? 1 : 0; // Convert "0" or "1" to integer 0 or 1
                insertTitleBasics.Parameters["@startYear"].Value = ParseInt(parts[5]);
                insertTitleBasics.Parameters["@endYear"].Value = ParseInt(parts[6]);
                insertTitleBasics.Parameters["@runtimeMinutes"].Value = ParseInt(parts[7]);
                insertTitleBasics.Parameters["@genres"].Value = TranslateNulls(parts[8]); // Assuming genres are already comma-separated or will be handled by TranslateNulls
                insertTitleBasics.ExecuteNonQuery();
            }

            foreach (var parts in _titleAkas)
            {
                insertTitleAkas.Parameters["@tconst"].Value = parts[0];
                insertTitleAkas.Parameters["@ordering"].Value = ParseInt(parts[1]);
                insertTitleAkas.Parameters["@title"].Value = parts[2];
                insertTitleAkas.Parameters["@region"].Value = TranslateNulls(parts[3]);
                insertTitleAkas.Parameters["@language"].Value = TranslateNulls(parts[4]);
                insertTitleAkas.Parameters["@types"].Value = TranslateNulls(parts[5]);
                insertTitleAkas.Parameters["@attributes"].Value = TranslateNulls(parts[6]);
                insertTitleAkas.Parameters["@isOriginalTitle"].Value = parts[7] == "1" ? 1 : 0;
                insertTitleAkas.ExecuteNonQuery();
            }

            foreach (var parts in _titlePrincipals)
            {
                insertTitlePrincipals.Parameters["@tconst"].Value = parts[0];
                insertTitlePrincipals.Parameters["@ordering"].Value = ParseInt(parts[1]);
                insertTitlePrincipals.Parameters["@nconst"].Value = parts[2];
                insertTitlePrincipals.Parameters["@category"].Value = TranslateNulls(parts[3]);
                insertTitlePrincipals.Parameters["@job"].Value = TranslateNulls(parts[4]);
                insertTitlePrincipals.Parameters["@characters"].Value = TranslateNulls(parts[5]);
                insertTitlePrincipals.ExecuteNonQuery();
            }

            foreach (var parts in _nameBasics)
            {
                insertNameBasics.Parameters["@nconst"].Value = parts[0];
                insertNameBasics.Parameters["@primaryName"].Value = parts[1];
                insertNameBasics.Parameters["@birthYear"].Value = ParseInt(parts[2]);
                insertNameBasics.Parameters["@deathYear"].Value = ParseInt(parts[3]);
                insertNameBasics.Parameters["@primaryProfession"].Value = TranslateNulls(parts[4]);
                insertNameBasics.Parameters["@knownForTitles"].Value = TranslateNulls(parts[5]);
                insertNameBasics.ExecuteNonQuery();
            }

            transaction.Commit();
        }

        sw.Stop();
        Console.WriteLine($"Finished adding to database in {sw.Elapsed.TotalSeconds:F2} seconds.");
    }

    private class XconstSet
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
            for (int i = 2; i < Xconst.Length; ++i)
            {
                id = id * 10 + (Xconst[i] - (byte)'0');
            }
            return id;
        }
    }

    private class GzipReader : IDisposable
    {
        private readonly FileStream _fileStream;
        private readonly GZipStream _gzipStream;
        private readonly byte[] _buffer;
        private readonly XconstSet _includedXconsts;

        private int _bytesInBuffer;  // Number of valid bytes currently in the buffer
        private int _readOffset;     // Where the next search for a line starts in the buffer

        private int _linesRead;
        private int _entriesReturned;

        public GzipReader(string filePath, XconstSet includedXconsts, int bufferSize = 128 * 1024)
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
                    return null;
                }

                int firstTabIndex = line.IndexOf((byte)'\t');
                if (firstTabIndex == -1)
                {
                    throw new InvalidDataException("Could not find Xconst field");
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
                    _readOffset += newlineIndex + 1; // Move past the consumed line (including \n)
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
                _bytesInBuffer += bytesRead;
            }
        }
    }
}