train-als:
	./trainals.sh train --in ${PWD}/ratings.csv --out-items items.csv

dot-pairs-cov:
	./datastats.sh pairstats --in items.csv --dot_out cov.dot \
		--cols ".*features.*" \
		--dot_weight cov
	neato -Tpdf ../cov.dot -o cov.pdf

dot-pairs-corr:
	./datastats.sh pairstats --in items.csv --dot_out corr.dot \
		--cols ".*features.*" \
		--dot_weight corr
	neato -Tpdf ../corr.dot -o corr.pdf

dot-pairs-linear:
	./datastats.sh pairstats --in items.csv --dot_out linear.dot \
		--cols ".*features.*" \
		--dot_weight linear
	neato -Tpdf ../linear.dot -o linear.pdf

stats:
	./datastats.sh stats --in items.csv \
		--cols ".*features.*"
