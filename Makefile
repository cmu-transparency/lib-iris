gen:
	make clean
	sbt "run --num-ratings 50000 --num-users 1000 --movies ml-20m.imdb.small.csv -o ratings.csv --seed 0"

gen_l2:
	make clean
	sbt "run --num-ratings 50000 --num-users 1000 --movies ml-20m.imdb.small.csv -o ratings.csv --seed 0 --num-latents 3 --num-apparents 14 --num-personalities 10"

clean:
	rm -Rf ratings.csv summary.txt

BASE_LOC := /Users/piotrm/Dropbox/repos/github/spfoundations/data-hmda
#BASE_LOC := /home/piotrm/repos/data-hmda

bench_forest:
	time sbt "runMain edu.cmu.spf.iris.BenchForest -i $(BASE_LOC)/2014.tsv -t \"Action Type\""
