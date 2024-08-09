# Slavic Search

This app creates reports.

## Setup

Clone the repo

```
git clone [address from above]
cd slavicsearch
```

copy .env-example to .env

```
cp .env-example .env
```

edit .env with actual environment variables.

Build container
```
docker build -t slavicsearchapp .
```

Run container with a shell
```
docker run -it --rm -v ${PWD}:/app slavicsearchapp
```

## Usage Extract Search Keys
Put your MARC binary file in the directory where you cloned the repository.
Give the command:  
```perl slvr_extract.pl -i [name of the MARC binary file] -o [<out_base>]```

An example:
```perl slvr_extract.pl -i slavic_search_2023052216_33680199200006381_new.mrc test2```

This will create two files:  
<out_base>_rpt.txt  
<out_base>.txt  

## Usage Search for Matches
The next script uses the search keys of the last script to search for matches:

```perl slvr_report.pl  -i <out_base>.txt -o slvr_<date>```

An example:
```perl slvr_report.pl  -i test2.txt -o slvr_20240808```

When you're done, type ‘exit’ and press Enter.
