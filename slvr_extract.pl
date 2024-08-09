#!/usr/bin/perl

use open qw( :encoding(UTF-8) :std );
use strict;
use File::Basename;
my $prgname = basename($0);
use Encode;
use Data::Dumper;

use Getopt::Std;

sub usage {
  my $msg = shift;
  $msg and $msg = " ($msg)";
  return "usage: $prgname -i input -o outbase $msg\n";
};

#our($opt_i, $opt_o, $opt_c, $opt_m, $opt_f, $opt_t);
our($opt_i, $opt_o);
getopts('i:o:');
$opt_i or die usage("no input file specified");
$opt_o or die usage("no output base specified");
my $infile = $opt_i;
my $outbase = $opt_o;
my $outfile = join('', $outbase, '.txt');
my $rptfile = join('', $outbase, '_rpt.txt');

open(OUT,">$outfile") or die "can't open $outfile for output: $!\n";
open(RPT,">$rptfile") or die "can't open $rptfile for output: $!\n";
binmode(OUT, ':utf8');

my $TITLE_ONLY = 0;

use MARC::Batch;

my $batch = MARC::Batch->new('USMARC',$infile);

my $today = getDate();
print RPT "infile is $infile\n";
print RPT "outfile is $outfile\n";

my (
  $reccnt,
  $no_008,
  $no_subj_cnt,
  $subj_cnt,
  $level_cnt,
  $no_996,
  $date_skip_cnt,
  $no_stdnum,
  $isbn_cnt,
  $outcnt,
  $f996_matched,
  $f996_search_cnt_skip,
  $f996_searched_cnt,
  $own_not_miu,
  $arrival_date,
  $no_arrival_date,
  $arrival_date_out_of_range,
  ) = (0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);

$batch->strict_off();
$batch->warnings_off();

my $exit = 0;
$SIG{INT} = sub { $exit = 1 };
$SIG{TERM} = sub { $exit = 1 };

my %date_source = ();
my $recID;
record: while (my $record = $batch->next() ) {
  $exit and do {
    print "exitting due to signal\n";
    last record;
  };
  $reccnt++;
  $recID = $record->field('001')->as_string() or die "$reccnt: no 001 field for record\n";

  $reccnt % 1000 == 0 and print STDERR "$reccnt ($recID): processing record\n";

  my $rec_fmt = get_fmt($record);

  my $field_008 = $record->field('008') or do {
    print "$reccnt: no 008 field for record\n";
    $no_008++;
    next record;
  };
  my $f008_date = substr($field_008->as_string(), 7, 4);
  my $f008_lang = substr($field_008->as_string(), 35, 3);
  
  my $search_keys = get_search_keys($record);

  my $field_245;
  my $title_ab = '';
  my $title_h = '';
  # get title from 245, subfield a,b
  $field_245 = $record->field('245') and do {
    my $nf_ind = $field_245->indicator(2);
    $title_ab = bytes::substr($field_245->as_string("ab"), $nf_ind);
    $title_ab = decode('utf8', $title_ab);
    $title_ab =~ s/^\s*(.*?)\s*$/$1/;  # clear any leading or trailing whitespace
    #$nf_ind > 0 and do {
    #  print "$recID: 245 nf ind is $nf_ind\n", $field_245->as_string('ab'), "\n$title_ab\n";
    #};
    $title_h = $field_245->as_string("h");
  };
  #$title_h and print "$reccnt ($recID): TITLE_H: $title_h\n";
  
  # get author from 1xx
  my $field_1xx;
  my $author = '';
  my $author_tag = '';
  $field_1xx = $record->field('1..') and do {
    $author = $field_1xx->as_string("a");
    $author_tag = $field_1xx->tag();
  };

  # get info from 260 field
  my $imprint = '';
  my $pub_date = '';
  my $field_260_264;
  $field_260_264 = $record->field('260|264') and do {
    # subfield b as imprint
    $imprint = $field_260_264->as_string("b");
    $pub_date = get_pub_date($field_260_264);
  };
  $pub_date or do {
    $pub_date = $f008_date;
    #print "$reccnt ($recID): date from 008 field:  $pub_date\n";
    $date_source{'date from 008'}++;
  };
  my $arrival_date = get_arrival_date($record) or do {
    $no_arrival_date++;
    next record;
  };
 
  my @out = ();
  push(@out, 
    "SYSNUM:$recID", 
    "FMT:$rec_fmt", 
    "LANG:$f008_lang", 
    "TITLE:$title_ab",
    "AUTHOR:$author",
    "AUTHOR_TAG:$author_tag",
    "DATE:$pub_date",
    "TITLE_H:$title_h",
    "IMPRINT:$imprint", 
    "ARRIVAL_DATE:$arrival_date",
    #"VENDOR:$vendor",
    #"FUND:$fund",
   );
  print OUT  join("\t", @out, @$search_keys), "\n";

  $outcnt++;
}

print RPT "$prgname--@ARGV\n";
print RPT "$reccnt records read\n";
print RPT "$no_008 no 008 field for record, skipped\n";
print RPT "$no_arrival_date no arrival date for record, skipped\n";
print RPT "$no_stdnum no stdnum in record\n";
print RPT "$outcnt records written\n";
print RPT "\n";

foreach my $date_source (sort keys %date_source) {
  print RPT "$date_source: $date_source{$date_source}\n";
}

sub getDate {
  my $inputDate = shift;
  if (!defined($inputDate)) { $inputDate = time; }
  my ($ss,$mm,$hh,$day,$mon,$yr,$wday,$yday,$isdst) = localtime($inputDate);
  my $year = $yr + 1900;
  $mon++;
  my $fmtdate = sprintf("%4.4d%2.2d%2.2d",$year,$mon,$day);
  return $fmtdate;
}

sub cutoffDate6 {
  my ($year, $month) = @_;
  $month <= 6 and do {
    $year--;
    $month += 12;
  };
  return sprintf("%4.4d%2.2d", $year, $month-6);
}

sub get_arrival_date {
  my $record = shift;

  my %arrival_dates = ();

  foreach my $field ($record->field('974')) {
    $field->as_string('c') eq 'IS-SEEES' or do {
      #print "$recID: non-IS-SEEES 974 field: ", outputField($field), "\n";
      next;
    };
    $field->as_string('r') and $arrival_dates{substr($field->as_string('r'), 0, 10)}++;
  }
  my @arrival_dates = sort keys %arrival_dates;
  scalar @arrival_dates or return '';
  scalar @arrival_dates > 1 and print "$recID: ", scalar @arrival_dates, " arrival dates in record: ", join(", ", @arrival_dates), "\n";
  return pop @arrival_dates;
}

sub get_search_keys {
  my $record = shift;
  my $search_keys = [];

  my %isbns = ();
  foreach my $f020 ($record->field('020')) {
    my $suba = $f020->as_string('a') or next;
    $isbns{$suba}++;
  }
  foreach my $isbn (keys(%isbns)) {
    push @$search_keys, join(':','ISBN',$isbn);
  }

  my %issns = ();
  foreach my $f022 ($record->field('022')) {
    my $suba = $f022->as_string('a') or next;
    $issns{$suba}++;
  }
  foreach my $issn (keys(%issns)) {
    push @$search_keys, join(':','ISSN',$issn);
  }

  my %oclc_numbers = ();
  foreach my $f035 ($record->field('035')) {
    my $suba = $f035->as_string('a') or next;
    $suba =~ /(oco{0,1}lc|ocm|ocn)/i or next;
    my ($oclc_num) = $suba =~ /(\d+)/ or next;
    $oclc_numbers{$oclc_num+0}++;
  }
  foreach my $oclc_number (keys(%oclc_numbers)) {
    push @$search_keys, join(':','OCLC',$oclc_number);
  }

  return $search_keys;
}

sub get_pub_date {
  my $field = shift;
  my $pub_date;
  $pub_date = $field->as_string("c") and do {
    $date_source{'260/4 subfield c'}++;
    return $pub_date;
  };
  # no subfield c--try to parse date from field string
  my $pub_text = $field->as_string();
  my @dates = $pub_text =~ /(\d{4})/g or do {
    #print "$recID: no dates in pub text: $pub_text\n";
    return '';
  };
  scalar @dates == 1 and do {
    $date_source{'260/4 single date from text'}++;
    return $dates[0];
  };
  $pub_text =~ /(\d{4}-\d{4})/ and do {
    my $date_range = $1;
    print "$recID: date range from 260: $pub_text: $date_range\n";
    $date_source{'260/4 date range from text'}++;
    return $date_range;
  }; 
  scalar @dates > 1 and do {
    print "$recID: $pub_text: ", join(", ", @dates), "\n";
    $date_source{'260/4 unknown multiple dates in from text'}++;
    return '';
  }
}

sub outputField {
    my $field = shift;
    my $newline = "\n";
    my $out = "";
    $out .= $field->tag()." ";
    if ($field->tag() lt '010') { $out .= "   ".$field->data; }
    else {
      $out .= $field->indicator(1).$field->indicator(2)." ";
      my @subfieldlist = $field->subfields();
      foreach my $sfl (@subfieldlist) {
        $out.="|".shift(@$sfl).shift(@$sfl);
      }
    }
    return $out;
}

sub get_fmt {
  my $record = shift;

  return 'BK';
}
