import { once } from "events";
import { createReadStream, createWriteStream } from "fs";
import readline from "readline";

/**
 * Fixes a malformed JSON file containing objects into a proper JSON array
 * Handles large files using streaming to avoid memory issues
 */
async function fixJsonFile(
    inputPath: string,
    outputPath: string,
): Promise<void> {
    const readStream = createReadStream(inputPath, { encoding: "utf-8" });
    const writeStream = createWriteStream(outputPath, { encoding: "utf-8" });

    // Create a readable line interface
    const rl = readline.createInterface({
        input: readStream,
        crlfDelay: Number.POSITIVE_INFINITY,
    });

    // Write the opening bracket for the JSON array
    writeStream.write("[\n");

    let isFirstObject = true;
    let lineBuffer = "";
    let openBraces = 0;
    let inQuotes = false;
    let escapeNext = false;

    for await (const line of rl) {
        lineBuffer += line;

        // Process the buffer character by character to track JSON structure
        for (let i = 0; i < line.length; i++) {
            const char = line[i];

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

                    // If we've closed an object
                    if (openBraces === 0) {
                        // If this isn't the first object, add a comma
                        if (!isFirstObject) {
                            writeStream.write(",\n");
                        } else {
                            isFirstObject = false;
                        }

                        // Write the complete object
                        writeStream.write(lineBuffer.trim());
                        lineBuffer = "";
                    }
                }
            }
        }

        // Add a newline if we're still collecting an object
        if (lineBuffer) {
            lineBuffer += "\n";
        }
    }

    // Write the closing bracket for the array
    writeStream.write("\n]");

    // Close streams
    writeStream.end();
    await once(writeStream, "finish");
    console.log(`JSON file fixed and saved to: ${ outputPath }`);
}

// Bun script entry point
if (import.meta.main) {
    const args = process.argv.slice(2);
    if (args.length !== 2) {
        console.error("Usage: bun run script.ts <input-file> <output-file>");
        process.exit(1);
    }

    const [inputFile, outputFile] = args;

    console.log(`Processing ${ inputFile }...`);
    fixJsonFile(inputFile, outputFile)
        .then(() => console.log("Completed successfully"))
        .catch((err) => {
            console.error("Error processing file:", err);
            process.exit(1);
        });
}

export { fixJsonFile };
