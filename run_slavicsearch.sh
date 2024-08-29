#!/bin/bash

# Look for .mrc files in the current working directory
shopt -s nullglob # Ensures that empty globs expand to an empty array
mrc_files=(*.mrc)

# Check if there are no .mrc files
if [ ${#mrc_files[@]} -eq 0 ]; then
  echo "No .mrc files found."
  exit 1
fi

# Check if there are more than one .mrc file
if [ ${#mrc_files[@]} -gt 1 ]; then
  echo "More than one .mrc file found."
  exit 1
fi

# Get the current date, hour, and minute in the format yyyymmddhhmm
NOW=$(date +"%Y%m%d")
FOLDERNOW=$(date +"%Y%m%d%H%M")

# Announce the action
echo "Starting run_slavicsearch.sh ... "
echo "  Input file = ${mrc_files[0]}"
echo "  Output basename = $NOW"

# Run the Perl command with the .mrc file and NOW variable
perl slvr_extract.pl -i "${mrc_files[0]}" -o "$NOW"

# Run the second Perl command with the NOW variable
perl slvr_report.pl -i "${NOW}.txt" -o "slvr_$NOW"

# Create a new folder named after the NOW variable
mkdir "$FOLDERNOW"

# Move files with specific extensions into the new folder
mv *.tsv *.txt *.mrc *.marc *.log "$FOLDERNOW" 2>/dev/null

# Exit message
echo "  Reports are in folder $FOLDERNOW"
echo "Ending run_slavicsearch.sh"

# Exit the script
exit 0
