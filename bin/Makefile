#JOBS_DIR := /Users/piotrm/Dropbox/repos/github/spfoundations/data-jobs/data
JOBS_DIR := /longterm/piotrm/data-jobs/data
JOBS_FILE := $(JOBS_DIR)/jobs.tsv
JOBS_NOTEXT_FILE := $(JOBS_DIR)/jobs.notext.tsv
APPS_FILE := $(JOBS_DIR)/apps.original.tsv
USERS_FILE := $(JOBS_DIR)/users.original.tsv
USERS_EXTENDED_FILE := $(JOBS_DIR)/users_extended.tsv

JOINED_PART_FILE := $(JOBS_DIR)/joined.tsv
JOINED_FILE := $(JOBS_DIR)/jobs.joined.tsv

job-stats:
	./datastats.sh pairstats --in $(JOINED_FILE)

HMDA := /longterm/piotrm/data-hmda/all_years.tsv
HMDA_BIGGEST := /longterm/piotrm/data-hmda/respondent_0000451965.tsv
HMDA_CLEANEST := /longterm/piotrm/data-hmda/all_years.morerelevant.tsv.d

hmda-dot-pairs-nmi:
	./datastats.sh pairstats --in $(HMDA_CLEANEST) --dot_out hmda-nmi.dot \
		--dot_weight nmi
	neato -Tpdf ../hmda-nmi.dot -o hmda-nmi.pdf

hmda-dot-pairs-nmi-0000617677:
	./datastats.sh pairstats --in $(HMDA_CLEANEST) --dot_out hmda-nmi-0000617677.dot \
		--dot_weight nmi --in-filter "\"\`Respondent ID\`=='0000617677'\""
	neato -Tpdf ../hmda-nmi-0000617677.dot -o hmda-nmi-0000617677.pdf

hmda-describe:
	./datautil.sh describe --in $(HMDA)

hmda-responders:
	./datautil.sh sql --in $(HMDA) \
	--out 'temp.csv' \
	--sql "\"select \
	\`Respondent ID\`, count(*) as cnt \
	from input \
	group by \`Respondent ID\` \
	order by cnt \
	\""

# biggest responders
# 0000617677	475107
# 26-2049351	514807
# 22-3039688	795266
# 75-2921540	834143
# 0000480228	905288
# 0000504713	993512
# 0000852218	1330146
# 26-4599244	1424048
# 7197000003	1526949
# 0000451965	3567655

hmda-getbiggest:
	./datautil.sh sql --in $(HMDA) \
	--out 'temp.csv' \
	--sql "\"select \
	* \
	from input \
	where \`Respondent ID\` = 0000451965 \
	\""

hmda-getrelevant:
	./datautil.sh sql --in $(HMDA_BIGGEST) \
	--out 'temp.csv' \
	--sql "\"select \
	* \
	from input \
	where \`Loan Purpose\` = 'Home purchase' \
	and \`Action Type\` != 'Loan purchased by the institution' \
	and \`Action Type\` != 'Application withdrawn by applicant' \
	and \`Action Type\` != 'Application approved but not accepted' \
	and \`Action Type\` != 'File closed for incompleteness' \
	\""
	bash combine_with_headers.sh ../temp.csv/part*.csv > /longterm/piotrm/data-hmda/respondent_0000451965.relevant.csv

convert-to-csv:
	./datautil.sh convert --in $(JOINED_FILE) --out $(JOINED_FILE).tsv

make-negative-apps:
	./datautil.sh sql \
	--in $(USERS_FILE) \
	--in-more $(APPS_FILE) \
	--in-more $(JOBS_FILE) \
	--out /longterm/piotrm/data-jobs/negatives.csv \
	--sql "\"select user.UserID, user.WindowID, user.Split, job.StartDate as ApplicationDate, job.JobID \
	from input user \
	full outer join input2 as job \
	where user.WindowID = job.WindowID \
	and not exists \
	(select * from input1 as appB \
          join input userB on appB.UserID = userB.UserID \
          where appB.WindowID = user.WindowID \
	  and userB.UserID = user.UserID \
	  and appB.JobID = job.JobID \
	) \""

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
