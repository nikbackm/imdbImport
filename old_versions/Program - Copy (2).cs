using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using Microsoft.Data.Sqlite; // Make sure you have this NuGet package installed

/*public class ImdbGzipDataImporter
{
    private readonly string _ratingsDbPath;
    private readonly string _imdbDataDirPath; // Directory where your .gz files are

    // HashSets to store filtered IDs
    private HashSet<string> _ratedTconsts = new HashSet<string>();
    private HashSet<string> _relevantNconsts = new HashSet<string>();

    private const int BatchSize = 50000; // Number of inserts per transaction commit

    public ImdbGzipDataImporter(string ratingsDbPath, string imdbDataDirPath)
    {
        _ratingsDbPath = ratingsDbPath;
        _imdbDataDirPath = imdbDataDirPath;
    }

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
            // Using SELECT DISTINCT in case 'const' column is not unique in your ratings table
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

    private void ProcessTitleAkas()
    {
        string filePath = Path.Combine(_imdbDataDirPath, "title.akas.tsv.gz");
        string tableName = "title_akas";
        Console.WriteLine($"\nProcessing {Path.GetFileName(filePath)} for table '{tableName}'...");
        Stopwatch sw = Stopwatch.StartNew();

        using (var connection = new SqliteConnection($"Data Source={_ratingsDbPath}"))
        {
            connection.Open();
            // Prepare the connection for bulk inserts
            // PRAGMA journal_mode = OFF; is generally good for large imports, but be aware of data loss on crash
            // You might want to use WAL mode for more robustness if crashes are a concern.
            // For now, sticking to OFF for pure speed
            using (var pragmaCmd = connection.CreateCommand())
            {
                pragmaCmd.CommandText = "PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;";
                pragmaCmd.ExecuteNonQuery();
            }

            // Clear existing data in the target table (optional, but good for re-runs)
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $"DELETE FROM {tableName};";
                cmd.ExecuteNonQuery();
                Console.WriteLine($"Cleared existing data in '{tableName}'.");
            }

            using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            using (GZipStream gzipStream = new GZipStream(fileStream, CompressionMode.Decompress))
            using (StreamReader reader = new StreamReader(gzipStream))
            {
                reader.ReadLine(); // Skip header line

                int rowCount = 0;
                int insertedCount = 0;
                var batchTimer = Stopwatch.StartNew();

                // Prepare the insert command parameters once
                using var insertCommand = connection.CreateCommand();
                insertCommand.CommandText = @"
                    INSERT INTO title_akas (titleId, ordering, title, region, language, types, attributes, isOriginalTitle)
                    VALUES (@titleId, @ordering, @title, @region, @language, @types, @attributes, @isOriginalTitle);
                ";
                insertCommand.Parameters.Add("@titleId", SqliteType.Text);
                insertCommand.Parameters.Add("@ordering", SqliteType.Integer);
                insertCommand.Parameters.Add("@title", SqliteType.Text);
                insertCommand.Parameters.Add("@region", SqliteType.Text);
                insertCommand.Parameters.Add("@language", SqliteType.Text);
                insertCommand.Parameters.Add("@types", SqliteType.Text);
                insertCommand.Parameters.Add("@attributes", SqliteType.Text);
                insertCommand.Parameters.Add("@isOriginalTitle", SqliteType.Integer);

                SqliteTransaction transaction = connection.BeginTransaction();
                insertCommand.Transaction = transaction;

                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    string[] parts = line.Split('\t');
                    rowCount++;

                    // Filter based on _ratedTconsts HashSet
                    if (parts.Length > 0 && _ratedTconsts.Contains(parts[0])) // parts[0] is titleId (tconst)
                    {
                        // Set parameters for the current row
                        insertCommand.Parameters["@titleId"].Value = parts[0];
                        insertCommand.Parameters["@ordering"].Value = int.TryParse(parts[1], out int orderingVal) ? orderingVal : DBNull.Value;
                        insertCommand.Parameters["@title"].Value = parts[2];
                        insertCommand.Parameters["@region"].Value = parts[3];
                        insertCommand.Parameters["@language"].Value = parts[4];
                        insertCommand.Parameters["@types"].Value = parts[5];
                        insertCommand.Parameters["@attributes"].Value = parts[6];
                        insertCommand.Parameters["@isOriginalTitle"].Value = (parts[7] == "1" ? 1 : 0);

                        insertCommand.ExecuteNonQuery();
                        insertedCount++;

                        if (insertedCount % BatchSize == 0)
                        {
                            transaction.Commit();
                            transaction.Dispose(); // Dispose previous transaction
                            transaction = connection.BeginTransaction(); // Start new one
                            insertCommand.Transaction = transaction; // Assign to command
                            Console.WriteLine($"  Processed {rowCount} lines. Inserted {insertedCount} Akas entries. Batch took {batchTimer.ElapsedMilliseconds} ms.");
                            batchTimer.Restart();
                        }
                    }
                }

                transaction.Commit(); // Commit any remaining inserts
                sw.Stop();
                Console.WriteLine($"Finished {Path.GetFileName(filePath)}. Total lines read: {rowCount}. Total Akas entries inserted: {insertedCount} in {sw.Elapsed.TotalSeconds:F2} seconds.");
            }
        }
    }

    private void ProcessTitlePrincipalsAndBuildNconsts()
    {
        string filePath = Path.Combine(_imdbDataDirPath, "title.principals.tsv.gz");
        string tableName = "title_principals";
        Console.WriteLine($"\nProcessing {Path.GetFileName(filePath)} for table '{tableName}' and building relevant NCONSTs...");
        Stopwatch sw = Stopwatch.StartNew();

        using (var connection = new SqliteConnection($"Data Source={_ratingsDbPath}"))
        {
            connection.Open();
            using (var pragmaCmd = connection.CreateCommand())
            {
                pragmaCmd.CommandText = "PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;";
                pragmaCmd.ExecuteNonQuery();
            }

            // Clear existing data in the target table (optional)
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $"DELETE FROM {tableName};";
                cmd.ExecuteNonQuery();
                Console.WriteLine($"Cleared existing data in '{tableName}'.");
            }

            using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            using (GZipStream gzipStream = new GZipStream(fileStream, CompressionMode.Decompress))
            using (StreamReader reader = new StreamReader(gzipStream))
            {
                reader.ReadLine(); // Skip header line

                int rowCount = 0;
                int insertedCount = 0;
                var batchTimer = Stopwatch.StartNew();

                // Prepare insert command for title_principals
                using var insertCommand = connection.CreateCommand();
                insertCommand.CommandText = @"
                    INSERT INTO title_principals (tconst, ordering, nconst, category, job, characters)
                    VALUES (@tconst, @ordering, @nconst, @category, @job, @characters);
                ";
                insertCommand.Parameters.Add("@tconst", SqliteType.Text);
                insertCommand.Parameters.Add("@ordering", SqliteType.Integer);
                insertCommand.Parameters.Add("@nconst", SqliteType.Text);
                insertCommand.Parameters.Add("@category", SqliteType.Text);
                insertCommand.Parameters.Add("@job", SqliteType.Text);
                insertCommand.Parameters.Add("@characters", SqliteType.Text);

                SqliteTransaction transaction = connection.BeginTransaction();
                insertCommand.Transaction = transaction;

                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    string[] parts = line.Split('\t');
                    rowCount++;

                    // Filter based on _ratedTconsts HashSet
                    if (parts.Length > 0 && _ratedTconsts.Contains(parts[0])) // parts[0] is tconst
                    {
                        // Add nconst to our _relevantNconsts HashSet for later name_basics filtering
                        if (parts.Length > 2) // Ensure nconst part exists
                        {
                            _relevantNconsts.Add(parts[2]); // parts[2] is nconst
                        }

                        // Set parameters for title_principals
                        insertCommand.Parameters["@tconst"].Value = parts[0];
                        insertCommand.Parameters["@ordering"].Value = int.TryParse(parts[1], out int orderingVal) ? orderingVal : DBNull.Value;
                        insertCommand.Parameters["@nconst"].Value = parts[2];
                        insertCommand.Parameters["@category"].Value = parts[3];
                        insertCommand.Parameters["@job"].Value = parts[4];
                        insertCommand.Parameters["@characters"].Value = parts[5];

                        insertCommand.ExecuteNonQuery();
                        insertedCount++;

                        if (insertedCount % BatchSize == 0)
                        {
                            transaction.Commit();
                            transaction.Dispose();
                            transaction = connection.BeginTransaction();
                            insertCommand.Transaction = transaction;
                            Console.WriteLine($"  Processed {rowCount} lines. Inserted {insertedCount} Principals entries. Current NCONSTs found: {_relevantNconsts.Count}. Batch took {batchTimer.ElapsedMilliseconds} ms.");
                            batchTimer.Restart();
                        }
                    }
                }

                transaction.Commit();
                sw.Stop();
                Console.WriteLine($"Finished {Path.GetFileName(filePath)}. Total lines read: {rowCount}. Total Principals entries inserted: {insertedCount} in {sw.Elapsed.TotalSeconds:F2} seconds.");
                Console.WriteLine($"Unique relevant NCONSTs collected: {_relevantNconsts.Count}");
            }
        }
    }

    private void ProcessNameBasics()
    {
        string filePath = Path.Combine(_imdbDataDirPath, "name.basics.tsv.gz");
        string tableName = "name_basics";
        Console.WriteLine($"\nProcessing {Path.GetFileName(filePath)} for table '{tableName}'...");
        Stopwatch sw = Stopwatch.StartNew();

        using (var connection = new SqliteConnection($"Data Source={_ratingsDbPath}"))
        {
            connection.Open();
            using (var pragmaCmd = connection.CreateCommand())
            {
                pragmaCmd.CommandText = "PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;";
                pragmaCmd.ExecuteNonQuery();
            }

            // Clear existing data in the target table (optional)
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $"DELETE FROM {tableName};";
                cmd.ExecuteNonQuery();
                Console.WriteLine($"Cleared existing data in '{tableName}'.");
            }

            using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            using (GZipStream gzipStream = new GZipStream(fileStream, CompressionMode.Decompress))
            using (StreamReader reader = new StreamReader(gzipStream))
            {
                reader.ReadLine(); // Skip header line

                int rowCount = 0;
                int insertedCount = 0;
                var batchTimer = Stopwatch.StartNew();

                // Prepare insert command for name_basics
                using var insertCommand = connection.CreateCommand();
                insertCommand.CommandText = @"
                    INSERT INTO name_basics (nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles)
                    VALUES (@nconst, @primaryName, @birthYear, @deathYear, @primaryProfession, @knownForTitles);
                ";
                insertCommand.Parameters.Add("@nconst", SqliteType.Text);
                insertCommand.Parameters.Add("@primaryName", SqliteType.Text);
                insertCommand.Parameters.Add("@birthYear", SqliteType.Integer);
                insertCommand.Parameters.Add("@deathYear", SqliteType.Integer);
                insertCommand.Parameters.Add("@primaryProfession", SqliteType.Text);
                insertCommand.Parameters.Add("@knownForTitles", SqliteType.Text);

                SqliteTransaction transaction = connection.BeginTransaction();
                insertCommand.Transaction = transaction;

                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    string[] parts = line.Split('\t');
                    rowCount++;

                    // Filter based on _relevantNconsts HashSet
                    if (parts.Length > 0 && _relevantNconsts.Contains(parts[0])) // parts[0] is nconst
                    {
                        // Set parameters for name_basics
                        insertCommand.Parameters["@nconst"].Value = parts[0];
                        insertCommand.Parameters["@primaryName"].Value = parts[1];
                        insertCommand.Parameters["@birthYear"].Value = int.TryParse(parts[2], out int birthYearVal) ? birthYearVal : DBNull.Value;
                        insertCommand.Parameters["@deathYear"].Value = int.TryParse(parts[3], out int deathYearVal) ? deathYearVal : DBNull.Value;
                        insertCommand.Parameters["@primaryProfession"].Value = parts[4];
                        insertCommand.Parameters["@knownForTitles"].Value = parts[5];

                        insertCommand.ExecuteNonQuery();
                        insertedCount++;

                        if (insertedCount % BatchSize == 0)
                        {
                            transaction.Commit();
                            transaction.Dispose();
                            transaction = connection.BeginTransaction();
                            insertCommand.Transaction = transaction;
                            Console.WriteLine($"  Processed {rowCount} lines. Inserted {insertedCount} Name Basics entries. Batch took {batchTimer.ElapsedMilliseconds} ms.");
                            batchTimer.Restart();
                        }
                    }
                }

                transaction.Commit(); // Commit any remaining inserts
                sw.Stop();
                Console.WriteLine($"Finished {Path.GetFileName(filePath)}. Total lines read: {rowCount}. Total Name Basics entries inserted: {insertedCount} in {sw.Elapsed.TotalSeconds:F2} seconds.");
            }
        }
    }
}

// Example Main method to run the importer:
public class Program
{
    public static void Main(string[] args)
    {
        // --- IMPORTANT: Adjust these paths ---
        string ratingsDatabasePath = "C:\\YourPath\\my_ratings.db"; // Your actual ratings database
        string imdbFilesDirectory = "C:\\YourPath\\imdb_gz_files"; // Directory where name.basics.tsv.gz, etc., are

        // --- Create dummy ratings.db and IMDb files for testing (if you don't have them) ---
        // This setup is for demonstration. In a real scenario, your ratings.db would exist with data.
        CreateDummyDatabasesAndFiles(ratingsDatabasePath, imdbFilesDirectory);

        // --- Run the importer ---
        var importer = new ImdbGzipDataImporter(ratingsDatabasePath, imdbFilesDirectory);
        importer.ImportAllRelevantImdbData();

        // --- Optional: Verify data in ratings.db after import ---
        Console.WriteLine("\n--- Verifying imported data ---");
        VerifyImportedData(ratingsDatabasePath);
    }


    // --- Helper methods for testing (you can remove these in your production code) ---
    private static void CreateDummyDatabasesAndFiles(string ratingsDbPath, string imdbFilesDir)
    {
        Console.WriteLine("\n--- Setting up dummy databases and GZ files for testing ---");

        // Ensure directory exists
        Directory.CreateDirectory(imdbFilesDir);

        // Create dummy ratings.db
        if (File.Exists(ratingsDbPath)) File.Delete(ratingsDbPath);
        using (var conn = new SqliteConnection($"Data Source={ratingsDbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS ratings (const TEXT PRIMARY KEY, rating REAL);
                CREATE TABLE IF NOT EXISTS title_akas (
                    titleId TEXT, ordering INTEGER, title TEXT, region TEXT, language TEXT,
                    types TEXT, attributes TEXT, isOriginalTitle INTEGER,
                    PRIMARY KEY (titleId, ordering)
                ) WITHOUT ROWID; -- Using WITHOUT ROWID for potential minor performance gain/smaller size
                CREATE TABLE IF NOT EXISTS title_principals (
                    tconst TEXT, ordering INTEGER, nconst TEXT, category TEXT, job TEXT, characters TEXT,
                    PRIMARY KEY (tconst, ordering)
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS name_basics (
                    nconst TEXT PRIMARY KEY, primaryName TEXT, birthYear INTEGER, deathYear INTEGER,
                    primaryProfession TEXT, knownForTitles TEXT
                ) WITHOUT ROWID;

                -- Insert some dummy ratings that the importer will filter on
                INSERT OR REPLACE INTO ratings (const, rating) VALUES ('tt0000001', 8.9);
                INSERT OR REPLACE INTO ratings (const, rating) VALUES ('tt0000002', 7.5);
                INSERT OR REPLACE INTO ratings (const, rating) VALUES ('tt0000003', 9.2); -- This title is rated
                INSERT OR REPLACE INTO ratings (const, rating) VALUES ('tt0000003', 9.5); -- Duplicate rating for tt0000003
                INSERT OR REPLACE INTO ratings (const, rating) VALUES ('tt0000004', 6.0);
            ";
            cmd.ExecuteNonQuery();
        }
        Console.WriteLine($"Dummy ratings.db created at {ratingsDbPath}");


        // Create dummy title.akas.tsv.gz
        string akasPath = Path.Combine(imdbFilesDir, "title.akas.tsv.gz");
        using (var fs = new FileStream(akasPath, FileMode.Create))
        using (var gz = new GZipStream(fs, CompressionMode.Compress))
        using (var writer = new StreamWriter(gz))
        {
            writer.WriteLine("titleId\tordering\ttitle\tregion\tlanguage\ttypes\tattributes\tisOriginalTitle"); // Header
            writer.WriteLine("tt0000001\t1\tAkas Title 1 US\tUS\ten\t\t\t0"); // Should be imported
            writer.WriteLine("tt0000001\t2\tAkas Title 1 GB\tGB\ten\t\t\t0"); // Should be imported
            writer.WriteLine("tt0000002\t1\tAkas Title 2\tUS\ten\t\t\t1"); // Should be imported
            writer.WriteLine("tt0000005\t1\tAkas Title 5 (Not Rated)\tUS\ten\t\t\t0"); // Should NOT be imported
        }
        Console.WriteLine($"Dummy {Path.GetFileName(akasPath)} created.");

        // Create dummy title.principals.tsv.gz
        string principalsPath = Path.Combine(imdbFilesDir, "title.principals.tsv.gz");
        using (var fs = new FileStream(principalsPath, FileMode.Create))
        using (var gz = new GZipStream(fs, CompressionMode.Compress))
        using (var writer = new StreamWriter(gz))
        {
            writer.WriteLine("tconst\tordering\tnconst\tcategory\tjob\tcharacters"); // Header
            writer.WriteLine("tt0000001\t1\tnm0000001\tdirector\t\t\t"); // Rated title, actor A
            writer.WriteLine("tt0000001\t2\tnm0000002\tactor\t\t[\"CharacterA\"]"); // Rated title, actor B
            writer.WriteLine("tt0000002\t1\tnm0000001\twriter\t\t"); // Rated title, actor A again (different role)
            writer.WriteLine("tt0000003\t1\tnm0000003\tactor\t\t[\"CharacterC\"]"); // Rated title, actor C
            writer.WriteLine("tt0000005\t1\tnm0000004\tdirector\t\t\t"); // Not rated title, actor D
        }
        Console.WriteLine($"Dummy {Path.GetFileName(principalsPath)} created.");

        // Create dummy name.basics.tsv.gz
        string namesPath = Path.Combine(imdbFilesDir, "name.basics.tsv.gz");
        using (var fs = new FileStream(namesPath, FileMode.Create))
        using (var gz = new GZipStream(fs, CompressionMode.Compress))
        using (var writer = new StreamWriter(gz))
        {
            writer.WriteLine("nconst\tprimaryName\tbirthYear\tdeathYear\tprimaryProfession\tknownForTitles"); // Header
            writer.WriteLine("nm0000001\tActor A\t1900\t1980\tdirector,writer\ttt0000001,tt0000002"); // Should be imported (from tt0000001, tt0000002)
            writer.WriteLine("nm0000002\tActor B\t1910\t\tactor\ttt0000001"); // Should be imported (from tt0000001)
            writer.WriteLine("nm0000003\tActor C\t1920\t\tactor\ttt0000003"); // Should be imported (from tt0000003)
            writer.WriteLine("nm0000004\tActor D\t1930\t\tactor\ttt0000005"); // Should NOT be imported (not in rated titles)
            writer.WriteLine("nm0000005\tActor E\t1940\t\tproducer\ttt0000006"); // Should NOT be imported
        }
        Console.WriteLine($"Dummy {Path.GetFileName(namesPath)} created.");
        Console.WriteLine("--- Dummy setup complete ---");
    }

    private static void VerifyImportedData(string ratingsDbPath)
    {
        using (var conn = new SqliteConnection($"Data Source={ratingsDbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();

            Console.WriteLine("\n--- Querying imported 'title_akas' ---");
            cmd.CommandText = "SELECT titleId, title FROM title_akas ORDER BY titleId, ordering;";
            using (var r = cmd.ExecuteReader())
            {
                while (r.Read()) Console.WriteLine($"Akas: {r.GetString(0)} - {r.GetString(1)}");
            }

            Console.WriteLine("\n--- Querying imported 'title_principals' ---");
            cmd.CommandText = "SELECT tconst, nconst, category FROM title_principals ORDER BY tconst, ordering;";
            using (var r = cmd.ExecuteReader())
            {
                while (r.Read()) Console.WriteLine($"Principal: {r.GetString(0)} - {r.GetString(1)} ({r.GetString(2)})");
            }

            Console.WriteLine("\n--- Querying imported 'name_basics' ---");
            cmd.CommandText = "SELECT nconst, primaryName FROM name_basics ORDER BY nconst;";
            using (var r = cmd.ExecuteReader())
            {
                while (r.Read()) Console.WriteLine($"Name: {r.GetString(0)} - {r.GetString(1)}");
            }
        }
    }
}*/