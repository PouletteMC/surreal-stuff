import { once } from "events";
import { createReadStream, createWriteStream } from "fs";
import readline from "readline";

interface Genre {
    id: number;
    name: string;
}

interface Collection {
    id: number;
    name?: string;
    poster_path?: string;
    backdrop_path?: string;
}

interface Movie {
    id: number;
    title?: string;
    budget?: number;
    overview?: string;
    poster_path?: string;
    release_date?: string;
    revenue?: number;
    runtime?: number;
    tagline?: string;
    vote_average?: number;
    original_language?: string;
    genres: Genre[];
    belongs_to_collection?: Collection;
}

/**
 * Converts a malformed JSON file with movie objects into optimized SurrealDB import statements
 * Implements bulk operations and batching for maximum ingest performance
 * Compatible with SurrealDB 2.2.1
 */
async function convertMoviesToOptimizedSurql(
    inputPath: string,
    outputPath: string,
    batchSize = 1000,
): Promise<void> {
    const readStream = createReadStream(inputPath, { encoding: "utf-8" });
    const writeStream = createWriteStream(outputPath, { encoding: "utf-8" });

    // Add header with optimization notes
    writeStream.write("-- Optimized SurrealDB Movie Import Statements\n");
    writeStream.write(`-- Generated on ${ new Date().toISOString() }\n`);
    writeStream.write(
        "-- Configured for maximum ingest performance with SurrealDB 2.2.1\n\n",
    );

    // Start transaction directly without the DEFINE OPTION statements
    writeStream.write("BEGIN TRANSACTION;\n\n");

    const rl = readline.createInterface({
        input: readStream,
        crlfDelay: Number.POSITIVE_INFINITY,
    });

    let objectBuffer = "";
    let openBraces = 0;
    let inQuotes = false;
    let escapeNext = false;
    let movieCount = 0;
    let batchCount = 0;
    let currentBatch: Movie[] = [];

    for await (const line of rl) {
        for (let i = 0; i < line.length; i++) {
            const char = line[i];
            objectBuffer += char;

            if (escapeNext) {
                escapeNext = false;
                continue;
            }

            if (char === "\\") {
                escapeNext = true;
                continue;
            }

            if (char === '"' && !escapeNext) {
                inQuotes = !inQuotes;
                continue;
            }

            if (!inQuotes) {
                if (char === "{") {
                    openBraces++;
                } else if (char === "}") {
                    openBraces--;

                    // If we've completed an object
                    if (openBraces === 0) {
                        try {
                            const movie = JSON.parse(objectBuffer) as Movie;
                            currentBatch.push(movie);
                            movieCount++;

                            // When we reach the batch size, write them as a bulk operation
                            if (currentBatch.length >= batchSize) {
                                writeBatchToFile(writeStream, currentBatch, batchCount);
                                currentBatch = [];
                                batchCount++;

                                console.log(
                                    `Processed ${ movieCount } movies (${ batchCount } batches)...`,
                                );
                            }

                            // Reset buffer for next object
                            objectBuffer = "";
                        } catch (err) {
                            console.error("Error parsing object:", err);
                            objectBuffer = "";
                        }
                    }
                }
            }
        }

        // Add newline if we're in the middle of an object
        if (objectBuffer && openBraces > 0) {
            objectBuffer += "\n";
        }
    }

    // Write any remaining movies in the final batch
    if (currentBatch.length > 0) {
        writeBatchToFile(writeStream, currentBatch, batchCount);
        batchCount++;
    }

    // Commit transaction
    writeStream.write("COMMIT TRANSACTION;\n");

    // Close streams
    writeStream.end();
    await once(writeStream, "finish");

    console.log(
        `Completed! Generated optimized SurrealDB import for ${ movieCount } movies in ${ batchCount } batches.`,
    );
    console.log(`Output saved to: ${ outputPath }`);
}

/**
 * Writes a batch of movies to the file as a bulk operation
 */
function writeBatchToFile(
    writeStream: NodeJS.WritableStream,
    movies: Movie[],
    batchNumber: number,
): void {
    // Add batch comment
    writeStream.write(`-- Batch ${ batchNumber + 1 }\n`);

    // Use LET statement for bulk insert
    writeStream.write("LET $movies = [\n");

    // Add all movies as an array of objects
    for (let i = 0; i < movies.length; i++) {
        const movie = movies[i];
        writeStream.write(formatMovieObject(movie));

        // Add comma for all but the last item
        if (i < movies.length - 1) {
            writeStream.write(",\n");
        } else {
            writeStream.write("\n");
        }
    }

    writeStream.write("];\n\n");

    // Bulk insert using FOR loop
    writeStream.write("FOR $movie IN $movies {\n");
    writeStream.write("  CREATE movie CONTENT $movie;\n");
    writeStream.write("};\n\n");
}

/**
 * Properly escapes strings for SurrealDB 2.2.1 without using \b
 */
function escapeStr(str: string | undefined): string {
    if (!str) return "";

    // Replace with proper SurrealDB string escaping
    // Avoiding \b which can cause issues
    return str
        .replace(/\\/g, "\\\\") // Escape backslashes first
        .replace(/'/g, "\\'") // Escape single quotes
        .replace(/\n/g, "\\n") // Escape newlines
        .replace(/\r/g, "\\r") // Escape carriage returns
        .replace(/\t/g, "\\t") // Escape tabs
        .replace(/\f/g, "\\f"); // Escape form feed
    // Removed \b escaping as requested
}

/**
 * Formats a movie object as a SurrealDB compatible object
 */
function formatMovieObject(movie: Movie): string {
    const title = escapeStr(movie.title || "");
    const overview = escapeStr(movie.overview || "");
    const tagline = escapeStr(movie.tagline || "");
    const poster = escapeStr(movie.poster_path || "/empty");
    const release_date = movie.release_date || "0001-01-01";
    const budget = movie.budget || 0;
    const revenue = movie.revenue || 0;
    const runtime = movie.runtime || 0;
    const vote_average = movie.vote_average || 0;
    const language = movie.original_language || "en";

    // Format genres
    const genres =
        movie.genres && movie.genres.length > 0
            ? `[${ movie.genres.map((genre) => `genre:${ genre.id }`).join(", ") }]`
            : "[genre:0]";

    // Handle collection
    const collection = movie.belongs_to_collection
        ? `collection:${ movie.belongs_to_collection.id }`
        : "NULL";

    // Return formatted object
    return `  {
    id: ${ movie.id },
    title: '${ title }',
    budget: ${ budget },
    overview: '${ overview }',
    poster: '${ poster }',
    release_date: <datetime>'${ release_date }',
    revenue: ${ revenue },
    runtime: ${ runtime },
    tagline: '${ tagline }',
    vote_average: ${ vote_average },
    language: language:${ language },
    genre: ${ genres },
    collection: ${ collection }
  }`;
}

// Bun script entry point
if (import.meta.main) {
    const args = process.argv.slice(2);
    if (args.length < 2 || args.length > 3) {
        console.error(
            "Usage: bun run script.ts <input-json-file> <output-surql-file> [batch-size]",
        );
        process.exit(1);
    }

    const [inputFile, outputFile] = args;
    const batchSize = args[2] ? Number.parseInt(args[2]) : 1000;

    console.log(
        `Processing ${ inputFile } into optimized SurrealDB import statements...`,
    );
    console.log(`Using batch size: ${ batchSize }`);

    convertMoviesToOptimizedSurql(inputFile, outputFile, batchSize)
        .then(() => console.log("Processing complete!"))
        .catch((err) => {
            console.error("Error processing file:", err);
            process.exit(1);
        });
}

export { convertMoviesToOptimizedSurql };
