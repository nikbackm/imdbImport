using Microsoft.Data.Sqlite;
using System.Text;
/*
namespace imdb
{
    public enum ImdbDataSetType
    {
        Unknown,
        NameBasics,
        TitleAkas,
        TitlePrincipals,
    }

    internal class Program2
    {
        static int Main2(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("File name missing");
                return 1;
            }

            var fn = args[0];

            using var reader = new StreamReader(fn, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 256*1024);
            var header = reader.EndOfStream ? null : reader.ReadLine();
            if (header == null)
            {
                Console.Error.WriteLine("Header missing");
                return 2;
            }

            var dataSetType = header.Split('\t') switch
            {
                ["nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"] => ImdbDataSetType.NameBasics,
                ["titleId", "ordering", "title", "region", "language", "types", "attributes", "isOriginalTitle"] => ImdbDataSetType.TitleAkas,
                ["tconst", "ordering", "nconst", "category", "job", "characters"] => ImdbDataSetType.TitlePrincipals,
                _ => ImdbDataSetType.Unknown
            };
            if (dataSetType == ImdbDataSetType.Unknown)
            {
                Console.Error.WriteLine("Unsupported file type");
                return 2;
            }

            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                if (string.IsNullOrWhiteSpace(line)) continue;

                //var parts = line.Split('\t');
            }

            return 0;
        }

        public void ImportTsvToSqlite(string tsvFilePath, string dbPath, string tableName)
        {
            // 1. SQLite setup
            using var connection = new SqliteConnection($"Data Source={dbPath}");
            connection.Open();

            // 2. Optional PRAGMA settings for speed (use with caution for intermediate DBs)
            using (var pragmaCmd = connection.CreateCommand())
            {
                pragmaCmd.CommandText = "PRAGMA journal_mode = OFF";
                pragmaCmd.ExecuteNonQuery();
                pragmaCmd.CommandText = "PRAGMA synchronous = OFF";
                pragmaCmd.ExecuteNonQuery();
            }

            // 3. Prepare your insert command outside the loop (reused for all batches)
            using var command = connection.CreateCommand();
            // Adjust CommandText and parameters for your actual schema, e.g., title.akas.tsv
            command.CommandText = $"INSERT INTO {tableName} (titleId, ordering, title, region, language, types, attributes, isOriginalTitle) VALUES (@titleId, @ordering, @title, @region, @language, @types, @attributes, @isOriginalTitle)";
            command.Parameters.Add(new SqliteParameter("@titleId", SqliteType.Text));
            command.Parameters.Add(new SqliteParameter("@ordering", SqliteType.Integer));
            command.Parameters.Add(new SqliteParameter("@title", SqliteType.Text));
            command.Parameters.Add(new SqliteParameter("@region", SqliteType.Text));
            command.Parameters.Add(new SqliteParameter("@language", SqliteType.Text));
            command.Parameters.Add(new SqliteParameter("@types", SqliteType.Text));
            command.Parameters.Add(new SqliteParameter("@attributes", SqliteType.Text));
            command.Parameters.Add(new SqliteParameter("@isOriginalTitle", SqliteType.Integer));

            int batchSize = 50000; // Adjust for optimal performance
            int rowCount = 0;

            // 4. Start the first transaction outside the loop
            SqliteTransaction? currentTransaction = null; // Declare outside so it can be reassigned

            try
            {
                currentTransaction = connection.BeginTransaction();
                command.Transaction = currentTransaction; // Assign the transaction to the command

                using (var reader = new StreamReader(tsvFilePath))
                {
                    // Read and discard header line
                    if (!reader.EndOfStream)
                    {
                        reader.ReadLine();
                    }

                    while (!reader.EndOfStream)
                    {
                        string line = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(line)) continue;

                        string[] parts = line.Split('\t');

                        // --- Data Parsing and \N to NULL Conversion ---
                        // Example for title.akas.tsv (adjust indices and types for your specific file)
                        // Ensure you handle potential IndexOutOfRangeException for malformed lines
                        // Or for optional trailing columns with '\N' that might shorten 'parts'
                        command.Parameters["@titleId"].Value = parts[0];
                        command.Parameters["@ordering"].Value = int.TryParse(parts[1], out int orderingVal) ? orderingVal : DBNull.Value;
                        command.Parameters["@title"].Value = parts[2];

                        command.Parameters["@region"].Value = (parts.Length > 3 && parts[3] == @"\N") ? DBNull.Value : (object)(parts.Length > 3 ? parts[3] : DBNull.Value);
                        command.Parameters["@language"].Value = (parts.Length > 4 && parts[4] == @"\N") ? DBNull.Value : (object)(parts.Length > 4 ? parts[4] : DBNull.Value);
                        command.Parameters["@types"].Value = (parts.Length > 5 && parts[5] == @"\N") ? DBNull.Value : (object)(parts.Length > 5 ? parts[5] : DBNull.Value);
                        command.Parameters["@attributes"].Value = (parts.Length > 6 && parts[6] == @"\N") ? DBNull.Value : (object)(parts.Length > 6 ? parts[6] : DBNull.Value);

                        if (parts.Length > 7 && parts[7] != @"\N" && int.TryParse(parts[7], out int isOriginalTitleVal))
                        {
                            command.Parameters["@isOriginalTitle"].Value = isOriginalTitleVal;
                        }
                        else
                        {
                            command.Parameters["@isOriginalTitle"].Value = DBNull.Value;
                        }


                        command.ExecuteNonQuery();
                        rowCount++;

                        // Commit in batches
                        if (rowCount % batchSize == 0)
                        {
                            currentTransaction.Commit(); // Commit the current transaction
                            currentTransaction.Dispose(); // Dispose of it

                            Console.WriteLine($"Imported {rowCount} rows...");

                            // Start a NEW transaction for the next batch
                            currentTransaction = connection.BeginTransaction();
                            command.Transaction = currentTransaction; // IMPORTANT: Re-assign the new transaction to the command
                        }
                    }
                }

                // Final commit for any remaining rows that didn't hit a batchSize multiple
                currentTransaction.Commit();
                Console.WriteLine($"Finished importing. Total rows: {rowCount}");
            }
            catch (Exception ex)
            {
                currentTransaction?.Rollback();
                Console.Error.WriteLine($"An error occurred during import: {ex.Message}");
                // Log the full exception details (ex.ToString())
            }
            finally
            {
                currentTransaction?.Dispose(); // Ensure the final transaction is disposed
            }
        }
    }
}
*/