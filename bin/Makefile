JOBS_DIR := /Users/piotrm/Dropbox/repos/github/spfoundations/data-jobs/data
JOBS_FILE := $(JOBS_DIR)/jobs.tsv
JOBS_NOTEXT_FILE := $(JOBS_DIR)/jobs.notext.tsv
APPS_FILE := $(JOBS_DIR)/apps.tsv
USERS_FILE := $(JOBS_DIR)/users.tsv
USERS_EXTENDED_FILE := $(JOBS_DIR)/users_extended.tsv

JOINED_PART_FILE := $(JOBS_DIR)/joined.tsv
JOINED_FILE := $(JOBS_DIR)/jobs.joined.tsv

job-stats:
	./datastats.sh pairstats --in $(JOINED_FILE)

convert-to-csv:
	./datautil.sh convert --in $(JOINED_FILE) --out $(JOINED_FILE).tsv

extend-users:
	./datautil.sh sql \
	--in $(USERS_FILE) \
	--in-more $(APPS_FILE) \
	--out $(USERS_EXTENDED_FILE) \
	--sql "\"select *, \
	(select min(appB.applicationDate) \
           from input1 appB \
           join input userB \
           where appB.UserID = userB.UserID and user.UserID = userB.UserID \
	) as userStart, \
	(select max(appB.applicationDate) \
           from input1 appB \
           join input userB \
	   where appB.UserID = userB.UserID and user.UserID = userB.UserID \
	) as userEnd \
	from input user \
	\""

FIELDS := \
	job.Title as jobTitle, job.City as jobCity, \
	job.State as jobState, job.Country as jobCountry, \
	job.Zip5 as jobZipCode, \
	user.City as userCity, user.Country as userCountry, \
	user.ZipCode as userZipCode, user.DegreeType, user.Major, \
	user.GraduationDate, user.WorkHistoryCount, \
	user.TotalYearsExperience, user.CurrentlyEmployed, \
	user.ManagedOthers, user.ManagedHowMany

join-jobs-apps-positives:
	./datautil.sh sql \
	--in $(JOBS_FILE) \
	--in-more $(APPS_FILE) \
	--in-more $(USERS_EXTENDED_FILE) \
	--out $(JOINED_PART_FILE).positive \
	--sql "\"select 1 as applied, $(FIELDS) \
	from input job join input1 app on job.JobID = app.JobID \
	           join input2 user on user.UserID = app.UserID \
	\""

join-jobs-apps-negatives:
	./datautil.sh sql \
	--in $(JOBS_FILE) \
	--in-more $(APPS_FILE) \
	--in-more $(USERS_EXTENDED_FILE) \
	--out $(JOINED_PART_FILE).negative \
	--sql "\"select 0 as applied, $(FIELDS) \
	from input job full outer join input2 user \
	where job.startDate >= user.userStart and job.startDate <= user.userEnd \
	and not exists \
	(select * from input jobB \
          join input1 appB on jobB.JobID = appB.JobID \
          join input2 userB on userB.UserID = appB.UserID \
          where job.JobID = appB.JobID and user.UserID = appB.UserID \
	) \
	\""


strip-jobs:
	./datautil.sh sql \
	--in $(JOBS_FILE) --out $(JOBS_DIR)/jobs.stripped.tsv \
	--sql "\"select Title, City, State, Country, Zip5 from input\""

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
