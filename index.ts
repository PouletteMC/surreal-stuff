import { RecordId, Surreal } from "surrealdb";
import movies from "./fixed.json" assert { type: "json" };

interface Genre {
    id: number;
    name: string;
}
const db = new Surreal();

await db.connect("ws://localhost:8000");

await db.use({
    namespace: "grooveguessr",
    database: "development",
});

await db.signin({
    username: "root",
    password: "ajhv2EeKQLgWU8dyA9tsR4",
});

for (const movie of movies) {
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

console.log("Done");