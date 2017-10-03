gen:
	make clean
	sbt "run --num-ratings 50000 --num-users 1000 --movies ml-20m.imdb.small.csv -o ratings.csv --seed 0"

gen_l2:
	make clean
	sbt "run --num-ratings 50000 --num-users 1000 --movies ml-20m.imdb.small.csv -o ratings.csv --seed 0 --num-latents 3 --num-apparents 14 --num-personalities 10"

clean:
	rm -Rf ratings.csv summary.txt


#  [info] Person (prefs: (voice-over-narration ∈ imdb_keywords,1.0),(voice-over-narration ∈ imdb_keywords,-1.0))

# Covered:
#  (feature 1/0, top 2 layers) [info] Person (prefs: (female-nudity ∈ imdb_keywords,1.0),(boyfriend-girlfriend-relationship ∈ imdb_keywords,-1.0))
#  (feature 1/0, top 2 layers) [info] Person (prefs: (corpse ∈ imdb_keywords,1.0),(male-nudity ∈ imdb_keywords,-1.0))
#  (feature 2, top 2 layers) [info] Person (prefs: (gun ∈ imdb_keywords,1.0),(character-name-in-title ∈ imdb_keywords,-1.0))
#  (feature 3, top 2 layers) [info] Person (prefs: (betrayal ∈ imdb_keywords,1.0),(shot-to-death ∈ imdb_keywords,-1.0))
#  (feature 4, top 2 layers) [info] Person (prefs: (police ∈ imdb_keywords,1.0),(doctor ∈ imdb_keywords,-1.0))
#  (feature 5, top 2 layers) [info] Person (prefs: (friend ∈ imdb_keywords,1.0),(fight ∈ imdb_keywords,-1.0))
#  (feature 6, top 2 layers) [info] Person (prefs: (black-comedy ∈ imdb_keywords,1.0),(explosion ∈ imdb_keywords,-1.0))
#  (feature 6, deep
#  (feature 2 surprise-ending (top 2), action not significant (deeper)) and (feature 6, both deep but in same branch)
#    [info] Person (prefs: (action ∈ imdb_genres,1.0),(surprise-ending ∈ imdb_keywords,-1.0))
#  (feature 7 repeats some personalities)
#  (feature 8, top 2 layers) [info] Person (prefs: (money ∈ imdb_keywords,1.0),(knife ∈ imdb_keywords,-1.0))
#  (feature 9 repeats feature 6)
#  (feature 10 repeats feature 8)
#  (feature 11 some repeats)

BASE_LOC := /Users/piotrm/Dropbox/repos/github/spfoundations/data-hmda

bench_forest:
	time sbt "runMain edu.cmu.spf.iris.BenchForest -i $(BASE_LOC)/sample.tsv -t \"Action Type\""
