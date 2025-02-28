import { RecordId, Surreal } from "surrealdb";
import collections from "../collection_data.json" assert { type: "json" };
import movies from "../fixed-movies.json" assert { type: "json" };
import shows from "../fixed-shows.json" assert { type: "json" };

// interface Genre {
//     id: number;
//     name: string;
// }
const db = new Surreal();

await db.connect("ws://localhost:8000");

console.log("Connected");

await db.use({
    namespace: "grooveguessr",
    database: "development",
});

await db.signin({
    username: "root",
    password: "root",
});

console.log("Logged in");

for (const movie of movies) {
    console.log(`Inserting movie ${ movie.title }`);
    await db.query(
        "CREATE movie SET id=$id, title=$title, budget=$budget, overview=$overview, poster=$poster, release_date=<datetime>$release_date, revenue=$revenue, runtime=$runtime, tagline=$tagline, vote_average=$vote_average, genre=$genre, language=$language, collection=$collection ;",
        {
            id: movie.id,
            title: movie.title ? movie.title : "",
            budget: movie.budget ? movie.budget : 0,
            overview: movie.overview ? movie.overview : "",
            poster: movie.poster_path ? movie.poster_path : "/empty",
            release_date: movie.release_date ? movie.release_date : "0001-01-01",
            revenue: movie.revenue ? movie.revenue : 0,
            runtime: movie.runtime ? movie.runtime : 0,
            tagline: movie.tagline ? movie.tagline : "",
            vote_average: movie.vote_average ? movie.vote_average : 0,
            language: movie.original_language ? new RecordId("language", movie.original_language) : new RecordId("language", "en"),
            genre: movie.genres.length > 0 ? movie.genres.map((genre: Genre) => new RecordId("genre", genre.id)) : [new RecordId("genre", 0)],
            collection: movie.belongs_to_collection
                ? new RecordId("collection", movie.belongs_to_collection.id)
                : null,
        },
    );
}

for (const show of shows) {
    console.log(`Inserting show ${ show.original_name }`);
    await db.query(
        "CREATE show SET id=$id, title=$title, overview=$overview, poster=$poster, first_air_date=<datetime>$first_air_date, tagline=$tagline, vote_average=$vote_average, genre=$genre, language=$language, seasons=<int>$seasons ;",
        {
            id: show.id,
            title: show.original_name ? show.original_name : "",
            overview: show.overview ? show.overview : "",
            poster: show.poster_path ? show.poster_path : "/empty",
            first_air_date: show.first_air_date ? show.first_air_date : "0001-01-01",
            tagline: show.tagline ? show.tagline : "",
            vote_average: show.vote_average ? show.vote_average : 0,
            language: show.original_language ? new RecordId("language", show.original_language) : new RecordId("language", "en"),
            genre: show.genres.length > 0 ? show.genres.map((genre: Genre) => new RecordId("genre", genre.id)) : [new RecordId("genre", 0)],
            seasons: show.seasons.length,
        },
    );
}

for (const collection of collections) {
    console.log(`Inserting collection ${ collection.name }`);
    await db.query(
        "CREATE collection SET id=$id, name=$name, overview=$overview, poster=$poster, backdrop=$backdrop, parts=$parts ;",
        {
            id: collection.id,
            name: collection.name ? collection.name : "",
            overview: collection.overview ? collection.overview : "",
            poster: collection.poster_path ? collection.poster_path : "/empty",
            backdrop: collection.backdrop_path ? collection.backdrop_path : "/empty",
        },
    );
}

console.log("Done");

await db.close();

console.log("Closed");

process.exit(0);