#!/usr/bin/perl

# Automated Search -- Slavic Receipts

$SIG{'__WARN__'} = sub { warn $_[0] unless (caller eq "MARC::Record"); };

use diagnostics;
use strict;
use open qw( :encoding(UTF-8) :std );
use File::Basename;
my $prgname = basename($0);

use Carp;
use ZOOM;
use Net::Z3950::ZOOM;
use MARC::Record;
use FileHandle;
use Encode;
use Getopt::Std;
#use Time::localtime;
use Unicode::Normalize;

my ($name, $path, $suffix) = File::Basename::fileparse($0);
use Dotenv;
Dotenv->load("$path.env");
my $user = $ENV{user};
my $pass = $ENV{pass};

my @match_type = (
# srch    title    author     date    type
[''     , 'yes'  , 'yes'    , 'yes' , 'matched'],
[''     , 'yes'  , 'no 100' , 'yes' , 'matched'],
[''     , 'near' , 'yes'    , 'yes' , 'near match (near/author/date)'],
[''     , 'near' , 'no 100' , 'yes' , 'near match (near/date)'],
[''     , 'near' , 'no 100' , 'no'  , 'near match (Different edition)'],
[''     , 'near' , 'no'     , 'yes' , 'near match (near/date)'],
[''     , 'near' , 'yes'    , 'no'  , 'near match (Different edition)'],
[''     , 'near' , 'no'     , 'no'  , 'near match (Different edition)'],
[''     , 'yes'  , 'yes'    , 'no'  , 'near match (Different edition)'],
[''     , 'yes'  , 'no'     , 'yes' , 'near match (Title/Date)'],
['ISBN' , 'yes'  , 'no'     , 'no'  , 'near match (Different edition)'],
#[''     , 'yes'  , 'no 100' , 'yes' , 'near match (Title/Date)'],
['ISBN' , 'yes'  , 'no 100' , 'no'  , 'near match (Different edition)'],
['ISBN' , 'no'   , 'yes'    , 'yes' , 'near match (ISBN/author/date)'],
['ISBN' , 'no'   , 'no'     , 'yes' , 'near match (ISBN/date)'],
['ISBN' , 'no'   , 'no 100' , 'yes' , 'near match (ISBN/date)'],
[''     , 'no'   , 'no'     , 'no'  , 'exclude'],
[''     , 'no'   , 'yes'    , 'no'  , 'exclude'],
['TITLE', 'yes'  , 'no'     , 'no'  , 'exclude'],
['TITLE', 'no'   , 'yes'    , 'yes' , 'exclude'],
['TITLE', 'no'   , 'no'     , 'yes' , 'exclude'],
[''     , 'no'   , 'no 100' , 'no'  , 'exclude'],
['TITLE', 'yes'  , 'no 100' , 'no'  , 'exclude'],
);

my %big_ten_institutions = (
  "UIU" => 1,
  "IUL" => 1,
  "NUI" => 1,
  "UMC" => 1,
  "EEM" => 1,
  "MNU" => 1,
  "LDL" => 1,
  "INU" => 1,
  "OSU" => 1,
  "UPM" => 1,
  "IPL" => 1,
  "NJR" => 1,
  "GZM" => 1,
  "CGU" => 1,
  "WAU" => 1,
);

my @oclc_stopwords = qw/
  a for in she was an from into so 
  were and had is than when are has 
  it that which as have its the with 
  at he not their would be her of there 
  you but his on this by if or to 
/;
my %oclc_stopwords; @oclc_stopwords{@oclc_stopwords} = @oclc_stopwords;
  
my %SEARCH_ATTR = ( 'ISSN' => '8', 'ISBN' => '7', 'MPN' => '51', 'TITLE' => '4', 'OCLC' => '12', 'LCCN' => '9' );
my $MAX_HITS = 50;
 
sub usage {
  my $msg = shift;
  $msg and $msg = " ($msg)";
  return "usage: $prgname -i input -o outbase [-R (restart) -d (debug)]$msg\n";
};

our( $opt_i, $opt_o, $opt_d, $opt_R,);

getopts('i:o:dR');
$opt_i or die usage("no input file");
my $infile = $opt_i;
$opt_o or die usage("no outbase specified");
my $outbase = $opt_o;
my $outfile = join('', $outbase, '.log');
my $marcfile = join('', $outbase, '.marc');
my $rptfile_summary = join('', $outbase, '_summary_rpt.tsv');
my $rptfile_match = join('', $outbase, '_match_rpt.tsv');
my $rptfile_near_match = join('', $outbase, '_near_match_rpt.tsv');
my $rptfile_no_match = join('', $outbase, '_no_match_rpt.tsv');
my $rptfile_not_found = join('', $outbase, '_not_found_rpt.tsv');
my $rptfile_toomany = join('', $outbase, '_toomany_rpt.tsv');
my $reviewfile = join('', $outbase, '_review.txt');

my $control_tag = 'SLV';
length($control_tag) == 3 or die usage("invalid control tag: $control_tag");

my $open_write_mode = ">";
my $restart_mode = 0;
$opt_R and do {
  $restart_mode++;
  $open_write_mode = ">>";
  print "running in restart mode\n";
};

my $check_title = 1;
my $check_author = 1;
my $check_date = 1;

open(IN,"<$infile") or die "can't open $infile for input: $!\n";

my $debug = 0;
$opt_d and do {
  $debug++;
  #open(DEBUG, $open_write_mode . "search_z3950_title.debug.txt");
  open(DEBUG, join('', $open_write_mode, $outbase, "_debug.txt"));
  DEBUG->autoflush(1);
};

my %term_not_fnd = ();
my %search_error = ();
my %record_error = ();
my %type_cnt = ();
my %term_max = ();
my $incnt = 0;
my $search_cnt = 0;
my $marc_out_cnt = 0;

my $counters = {
  'match_selected' => 0,
  'near_match_selected' => 0,
  'tot_not_found' => 0,
  'not_selected' => 0,
  'toomany_cnt' => 0,
};

$restart_mode and do {
  # open output file and read last record
  open(OUT_OLD, "<$outfile") or die "can't open $outfile in input mode: $!\n";
  my $last_line;
  my $last_sysnum = 0;
  while (<OUT_OLD>) {
    chomp($last_line = $_);
  }
  $counters = parse_line($last_line);
  $last_sysnum = $counters->{'SYSNUM'};
  print "last line: $last_line\n";
  print "last sysnum: $last_sysnum\n";
  close OUT_OLD;
  # read input file, position at last processed record
  FIND_LINE:while(<IN>) {
    $incnt++;
    chomp();
    my $in = parse_line($_);
    my $sysnum = $in->{'SYSNUM'};
    $sysnum eq $last_sysnum and do {
      print "restart sysnum $last_sysnum found on line $incnt of input file\n";
      last FIND_LINE;
    };
  }
  print "restart mode, starting on line $incnt\n";
};

my $OUT = open_fh($outfile, $open_write_mode); 
my $REVIEW = open_fh($reviewfile, $open_write_mode); 
my $MARC = open_fh($marcfile, $open_write_mode); 
my $RPT_MATCH = open_fh($rptfile_match, $open_write_mode); 
my $RPT_NEAR_MATCH = open_fh($rptfile_near_match, $open_write_mode); 
my $RPT_NOT_FOUND = open_fh($rptfile_not_found, $open_write_mode); 
my $RPT_NO_MATCH = open_fh($rptfile_no_match, $open_write_mode); 
my $RPT_TOOMANY = open_fh($rptfile_toomany, $open_write_mode); 
my $RPT_SUMMARY = open_fh($rptfile_summary, '>'); 	# always write whole file

$restart_mode or do {
  print_report_header($RPT_MATCH);
  print_report_header($RPT_NEAR_MATCH);
  print_report_header($RPT_NOT_FOUND);
  print_report_header($RPT_NO_MATCH);
  print_report_header($RPT_TOOMANY);
};

#####
my $conn;
$conn = Create_conn_to_Zserver();
#####

my $today = getDate();

my $exit = 0;
$SIG{INT} = sub { $exit = 1 };
$SIG{TERM} = sub { $exit = 1 };

my $sysnum;
my $line;
LINE:while(<IN>) {
  $exit and do {
    print "exitting due to signal\n";
    last LINE;
  };

  chomp($line = $_);
  $incnt++;
  #$incnt > 250 and last LINE;

  ##### Re-connect Z-server to avoid timeout from OCLC.
  if (($incnt % 50) == 0) {
    $conn->destroy();
    print STDERR "$incnt: sleeping\n";
    sleep 2;
    print STDERR "$incnt: continue\n";
    $conn = Create_conn_to_Zserver();
  }

  my $in = parse_line($line);
  $sysnum = $in->{'SYSNUM'};
  $counters->{'SYSNUM'} = $sysnum;
  my $alma_title = $in->{'TITLE'};
  my $alma_author = $in->{'AUTHOR'};
  my $alma_date = $in->{'DATE'};
  my $arrival_date = $in->{'ARRIVAL_DATE'};
  my $vendor = $in->{'VENDOR'};
  my $fund = $in->{'FUND'};
  if ($in->{'TITLE_H'} eq '') {
    $in->{'HAS_245H'} = 0;
  } else {
    $in->{'HAS_245H'} = 1;
  }
  
  my $records = [];
  my $id_list = {};
  my @too_many = ();
  my $rec_info = {};
  SEARCH:foreach my $search ('OCLC', 'ISSN', 'ISBN', 'TITLE') {
    defined($in->{$search}) and do {
      my $term = $in->{$search};
      $term or die "$sysnum($incnt): no term for search $search\n";
      my $search_rc = search($search, $term, $records, $id_list, $in);
      $search_rc == 2 and push @too_many, "$search = '$term'";
      $search_cnt++;
    };
  } 
  scalar @$records or do {
    scalar(@too_many) and do {
      print "$sysnum($incnt): no records, too many records found for searches: ", join(",", @too_many), " \n";
      print STDERR "$sysnum($incnt): no records, too many records found for searches: ", join(",", @too_many), " \n";
      print_alma_info($RPT_TOOMANY, $in, $rec_info, "too many matches");
      $counters->{toomany_cnt}++;
      my $log_line = unparse_line($counters);
      print $OUT $log_line, "\n";
      next LINE;
    };
    $counters->{tot_not_found}++;
    print "$sysnum($incnt): no records for any search terms\n";
    print_alma_info($RPT_NOT_FOUND, $in, $rec_info, "not found");
    my $log_line = unparse_line($counters);
    print $OUT $log_line, "\n";
    next LINE;
  };
  my $num_found = scalar @$records;

  $rec_info = get_records($records, $in) or do {
    print "$sysnum($incnt):no valid records in results from server\n";
    print STDERR "$sysnum($incnt):no valid records in results from server\n";
  };

  my $match_type = $rec_info->{result_match_type};
  $match_type or do {
    $counters->{not_selected}++;
    print "$sysnum($incnt): no valid records found\n";
    print STDERR "$sysnum($incnt): no valid records found\n";
    print_alma_info($RPT_NO_MATCH, $in, $rec_info, "no matches");
    my $log_line = unparse_line($counters);
    print $OUT $log_line, "\n";
    next LINE; 
  };
  $counters->{selected}++;

  if ($match_type eq 'matched') {
    $counters->{match_selected}++;
    my $pm_lines = print_match($RPT_MATCH, $rec_info, $in);
    $pm_lines or die "$sysnum: no matches printed for match type $match_type\n";
  } elsif ($match_type eq 'near match') {
    $counters->{near_match_selected}++;
    print_near_match($RPT_NEAR_MATCH, $rec_info, $in);
  } else {
    die "$sysnum: no match\n";
  }

  my $log_line = unparse_line($counters);
  print $OUT $log_line, "\n";
}

print "'==============================\n";
print "$incnt records read\n";
$search_cnt and 		print "$search_cnt searches\n";
foreach my $type (sort keys %type_cnt) {
  print "\t$type: $type_cnt{$type}\n";
}

print $RPT_SUMMARY join("\t", "Slavic search summary results, run date is $today", "***"), "\n";
print $RPT_SUMMARY join("\t", "****************************", "***"), "\n";
print $RPT_SUMMARY join("\t", "Alma records searched:", $incnt), "\n";
print $RPT_SUMMARY join("\t", "OCLC record found, selected for Alma record:", $counters->{match_selected}), "\n";
print $RPT_SUMMARY join("\t", "OCLC record(s) found, near match selected for Alma record:", $counters->{near_match_selected}), "\n";
print $RPT_SUMMARY join("\t", "OCLC record(s) found, none selected for Alma record:", $counters->{not_selected}), "\n";
print $RPT_SUMMARY join("\t", "Too many matches found for Alma record:", $counters->{toomany_cnt}), "\n";
print $RPT_SUMMARY join("\t", "No OCLC record found for Alma record:", $counters->{tot_not_found}), "\n";

sub search {
  my $search = shift;
  my $term = shift;
  my $records = shift;
  my $id_list = shift;
  my $in = shift;

  my $norm_term = norm($term,$search);
  my $zq = "\@attr 1=$SEARCH_ATTR{$search} \"$norm_term\"";
  print "$sysnum($incnt):$term: searching $search, zq=$zq\n";
  my $rs;
  eval { $rs = $conn->search_pqf($zq) };
  $@ and do {
    print STDERR "$sysnum($incnt):search error trapped, search is $zq\n";
    print STDERR $@, "\n";
    exit;
  };
  $rs or do {
    my ($errcode, $errmsg, $addinfo, $diagset) = $conn->error_x();
    my $str = "$addinfo, $diagset";
    print STDERR "$sysnum($incnt):$term: conn->search error, $errcode ($errmsg), error occurred in $str\n";
    print "$sysnum($incnt):$term: conn->search error\n";
  };
  my $numrec = $rs->size();
  $debug and print DEBUG "$sysnum($incnt): search on $search=$norm_term, $numrec hits\n";
  $numrec ==  0 and do {
    print "$sysnum($incnt):$term: $search not found\n";
    #print $OUT "SEARCH:$search\t$line\n";
    return 0;
  };
  $numrec > $MAX_HITS and do {
    print "$sysnum($incnt):$term: too many records found for $search ($numrec)\n";
    #print $OUT "SEARCH:$search\t$line\n";
    return 2;
  };
  my $rs_size;
  eval { $rs_size = $rs->size(); };
  $@ and do {
    print STDERR "result set error trapped\n";
    print STDERR $@, "\n";
    exit;
  };
  
  ZREC:foreach my $i ( 0..$rs_size-1 ) {
    my $recnum = $i+1;
    my $zrec;
    eval { $zrec = $rs->record($i) };
    $@ and do {
      print STDERR "retrieval error trapped\n";
      print STDERR $@, "\n";
      exit;
    };
    $zrec or do {
      print "$sysnum($incnt): error getting record $recnum of $rs_size found for $search $term from z39.50 server\n";
      next ZREC;
    };
    my $zreclen = length($zrec->raw());
    $zreclen <= 5 and do {
      print STDERR "bad record, ignoring; length was $zreclen\n";
      print STDERR "raw data:\n" . $zrec->raw() . "\n";
      next ZREC;
    };
    my $mrec = MARC::Record->new_from_usmarc($zrec->raw());

    my $rec_sysnum = $mrec->field('001')->as_string();
    $rec_sysnum =~ s/oc[mn]//;

    exists $id_list->{$rec_sysnum} and next ZREC;
    $id_list->{$rec_sysnum} = $rec_sysnum;
    push @$records, [$search, $mrec];
  }
  return 1;
}

sub get_records {
  my $all_records = shift;
  my $in = shift;

  my $info_entries = [];
  my $rec_info = {};
  my $alma_title = $in->{TITLE};
  my $alma_author = $in->{AUTHOR};
  my $alma_author_tag = $in->{AUTHOR_TAG};
  my $alma_date = $in->{DATE};
  my $sysnum = $in->{SYSNUM};
  my $alma_title_norm = norm($alma_title, 'TITLE');
  my $alma_author_norm = '';
  my $alma_date_norm = '';
  my $rec_author_norm = '';
  my $rec_title_norm = '';
  my $rec_date_norm = '';
  my $f1xx;
  my %match_type = ();
  my @inst_counts = ();
  my @eym_counts = ();

  $check_author and $alma_author and $alma_author_tag eq '100' and $alma_author_norm = norm($alma_author,'AUTHOR', $alma_author_tag);
  $check_date and $alma_date and $alma_date_norm = norm($alma_date,'DATE');
  #$debug and print DEBUG "$sysnum($incnt): alma_author: $alma_author, alma_title: $alma_title\n";
  REC:foreach my $record_set (@$all_records) {
    my ($search, $mrec) = @$record_set;
    my $info_entry = {};
    my $rec_fmt = getRecFmt($mrec);
    my $rec_sysnum = $mrec->field('001')->as_string();
    #$rec_sysnum =~ s/oc[mn]//;
    $rec_sysnum =~ s/(ocm|ocn|on)//;
    my $f008 = $mrec->field('008')->as_string;
    my $leader = $mrec->leader();
    $info_entry->{elvl} = substr($leader, 17, 1);
    # check 040 field--keep if 040 |b is eng or no 040
    my $f040;
    $f040 = $mrec->field('040') and do {
      my $sub_b = $f040->as_string('b');
      $sub_b eq 'eng' or do {
        $debug and print DEBUG "$rec_sysnum: reject, 040 subfield b not eng: $sub_b\n";
        next REC;  
      };
    };
    # check 245 subfield h
    my $has_245h = 0;
    my $f245;
    $f245 = $mrec->field('245') and do {
      my $sub_h = $f245->as_string('h');
      $sub_h and do {
        $has_245h = 1;
        $debug and print DEBUG "$rec_sysnum: 245 subfield h: $sub_h\n";
      };
    };
    # check 008/23: must be blank
    substr($f008, 23, 1) eq ' ' or do {
      $debug and print DEBUG "$rec_sysnum: reject, 008/23 not blank: ", substr($f008, 23, 1) ,"\n";
      next REC;  
    };
    my $author_match = 'no 100';	# implies no 100 field in Alma record
    my $author_100 = '';
    $info_entry->{rec_author} = 'none';
    $f1xx = $mrec->field('1..') and do {
      $info_entry->{rec_author} = $f1xx->as_string('a');
      $f1xx->tag() eq '100' and $author_100 = $f1xx->as_string('a');
    };
    $check_author and $alma_author_norm and do {
      $author_match = 'no';	# default to no match
      $rec_author_norm = '';
      $author_100 and do {
        $rec_author_norm = norm($author_100, 'AUTHOR', '100');
      };
      if (field_match($alma_author_norm,$rec_author_norm)) {
        $author_match = 'yes';
        $debug and print DEBUG "$rec_sysnum: author match: alma $alma_author_norm, rec: $rec_author_norm\n";
      } else {
        $author_match = 'no';
        $debug and print DEBUG "$rec_sysnum: no author match: alma $alma_author_norm, rec: $rec_author_norm\n";
      }
    }; 
    my $title_match = 'no';
    $check_title and do {
      # get title from 245, subfield a,b
      my $rec_field_245 = $mrec->field('245');
      my $rec_nf_ind = $rec_field_245->indicator(2);
      my $rec_title_ab = bytes::substr($rec_field_245->as_string("ab"), $rec_nf_ind);
      $rec_title_ab = decode('utf8', $rec_title_ab);
      $rec_title_ab =~ s/^\s*(.*?)\s*$/$1/;  # clear any leading or trailing whitespace
      $info_entry->{rec_title} = $rec_title_ab;

      $rec_title_norm = norm($rec_title_ab,'TITLE');
      $title_match = field_match_word($alma_title_norm, $rec_title_norm, 5) or do {
        $debug and print DEBUG "$rec_sysnum: no title match: Alma: $alma_title_norm, rec: $rec_title_norm\n";
        next REC;
      };
      $debug and print DEBUG "$rec_sysnum: title match, rc=$title_match: alma $alma_title_norm, rec: $rec_title_norm\n";
      print "title_match, rc=$title_match\n";
    };
    my $rec_pub_date;
    my $rec_pub_date_source;
    ($rec_pub_date, $rec_pub_date_source) = get_pub_date($mrec);
    my $rec_pub_date_norm = norm($rec_pub_date, 'DATE');
    $info_entry->{rec_pub_date} = $rec_pub_date_norm;
    $info_entry->{rec_pub_date_source} = $rec_pub_date_source;
    #my $date_match = 'no alma date';
    my $date_match = 'no';
    $check_date and $alma_date_norm and do {
      if ($alma_date_norm eq $rec_pub_date_norm)  {
        $date_match = 'yes';
      } else {
        $debug and print DEBUG "no date match: alma: $sysnum, $alma_date_norm, rec: $rec_sysnum, $rec_pub_date_norm\n";
        print $REVIEW join("\t", $sysnum, $rec_sysnum, $rec_fmt, $alma_date, $rec_pub_date) . "\n";
        $date_match = 'no';
      } 
    };
    my ($institutions, $eym_cnt, $big_ten_institutions) = process_institutions($mrec);
    $info_entry->{search} = $search;
    $info_entry->{title_match} = $title_match;
    $info_entry->{date_match} = $date_match;
    $info_entry->{author_match} = $author_match;
    $info_entry->{has_245h} = $has_245h;
    $info_entry->{institutions} = $institutions;
    $info_entry->{big_ten_institutions} = $big_ten_institutions;
    $info_entry->{rec_lang} = substr($f008, 35, 3);
    $info_entry->{oclc_country_code} = substr($f008, 15, 3);
    $info_entry->{rec_oclc_num} = $rec_sysnum;
    $info_entry->{rec_fmt} = $rec_fmt;
    $info_entry->{eym_cnt} = $eym_cnt;
    $f040 and $info_entry->{rec_040_b} = $f040->as_string('b');
    my $f043 = $mrec->field('043'); 
    $f043 and $info_entry->{oclc_geo_code} = $f043->as_string();

    my $f050 = $mrec->field('050');
    $f050 and do {
      $info_entry->{f050} = $f050->as_string('ab');
      #print STDERR outputField($f050), "\n";
      $f050->indicator(2) eq '0' and do {
        #print STDERR "$rec_sysnum, DLC\n";
        $info_entry->{dlc} = 'DLC';
      };
    };
    my $f042 = $mrec->field('042');
    $f042 and do {
      #print STDERR outputField($f042), "\n";
      $f042->as_string('a')  eq 'pcc' and do {
        #print STDERR "$rec_sysnum, pcc\n";
        $info_entry->{pcc} = 'PCC';
      };
    };
    
    F490:foreach my $field ($mrec->field('490')) {
      $field->as_string('v') and do {
        $info_entry->{oclc_numbered_series} = '490$v';
        last F490;
      };
    }

    my $match_type = set_match_type($info_entry);
    $match_type and $match_type ne 'exclude' and do {
      $info_entry->{match_type} = $match_type;
      if ($match_type =~ /^near match/) {
        $match_type{'near match'}++;
      } else {
        $match_type{$match_type}++;
      }
      push @inst_counts, scalar @$institutions;
      push @eym_counts, $eym_cnt;
      push @$info_entries, $info_entry;
      print $MARC $mrec->as_usmarc();  
    }
  }

  my $result_match_type = '';
  if ( $match_type{'matched'} ) {
    $result_match_type = 'matched';
  } elsif ($match_type{'near match'}) {
    $result_match_type = 'near match';
  } else {
    $result_match_type = '';
  }
 
  $rec_info->{info_entries} = $info_entries;
  $rec_info->{result_match_type} = $result_match_type;
  $rec_info->{inst_count_list} = join(',', @inst_counts);
  $rec_info->{eym_count_list} = join(',', @eym_counts);
  return $rec_info;
}

sub norm {
  my $term = shift;
  my $type = uc(shift);
  my $author_tag = '';
  $type eq 'AUTHOR' and $author_tag = shift;
  NORM: {
    $type eq 'OCLC' and do {
      my $norm_term = '';
      ($norm_term) = $term =~ /(\d+)/;
      return $norm_term + 0;
    };
    $type eq 'LCCN' and do {
      my $norm_term = '';
      ($norm_term) = $term =~ /(\d+)/;
      return $norm_term;
    };
    $type eq 'ISBN' and do {
      $term =~ tr/- \'//d; # strip
      return lc($term);
    };
    $type eq 'ISSN' and do {
      $term =~ tr/- \'//d; # strip
      return lc($term);
    };
    $type eq 'MPN' and do {
      # default--no normalization
      return($term);
    };
    $type eq 'TITLE' and do {
      $term = lc($term);
      $term = char_norm_unicode($term);
      $term =~ s/ [-&:] / /g;
      $term =~ s/-/ /g;
      $term =~ tr/!@#$%^*()_+={}[];:'"<>,.?\xC5\xC6//d; 
      $term =~ tr (/) ( );
      my @wl = ();
      WORD:foreach my $word (split(/\s+/,$term)) {
        next WORD if $oclc_stopwords{$word};
        push(@wl, $word); 
      }
      return join(" ",@wl);
    };
    $type eq 'AUTHOR' and do {
      my $save_term = $term;
      $term = lc($term);
      $author_tag eq '100' and do {
        $term =~ /,/ and do {
          $term =~ s/(.*?,\s*\w{1}).*/$1/;
        };
      };
      $term = char_norm_unicode($term);
      $term =~ s/ [-&:] / /g;
      $term =~ s/-/ /g;
      #$term =~ tr/!@#$%^*()_+={}[];:'"<>,.?\xC5\xC6//d; 
      $term =~ tr/!@#$%^*()_+={}[];:'"<>,.?//d; 
      $term =~ tr (/) ( );
      #my @wl = ();
      #WORD:foreach my $word (split(/\s+/,$term)) {
      #  next WORD if $oclc_stopwords{$word};
      #  push(@wl, $word); 
      #}
      #print  "author $author_tag: $save_term ->  " , join(" ",@wl), "\n";
      #return join(" ",@wl);
      return $term;
    };
    $type eq 'DATE' and do {
      my $save_date = $term;
      $term =~ /(\d{4})/ and return $1;
      $term =~ tr/0-9u-//cd;
      return $term;
    };
    die "invalid normalization type: $type\n";
  }
}

sub char_norm_marc8 {
  my $term = shift;
  $term =~ tr/\xE0-\xFE//d;			# diacritics
  $term =~ s/\xA1|\xB1|\xBE/l/g;		# polish l, script l
  $term =~ s/\xA2|\xB2|\xAC|\xBC/o/g;		# o w/slash, hooked o
  $term =~ s/\xA3|\xB3|\xBA/d/g;		# d w/ crossbar, eth
  $term =~ s/\xA4|\xB4/th/g;			# icelandic thorn
  $term =~ s/\xA5|\xB5/ae/g;			# ae
  $term =~ s/\xA6|\xB6/oe/g;			# oe
  $term =~ s/\xAD|\xBD/u/g;			# hooked u
  return $term;
}

sub char_norm_unicode {
  my $term = shift;
  $term = Unicode::Normalize::NFKD($term);
  #$term =~ s/\p{NonspacingMark}//g;
  $term =~ s/\p{M}//g;          # Mark  
  $term =~ s/\p{Lm}//g;         # Modifier_letter
  $term =~ s/\x{142}/l/g;	# l with slash
  $term =~ s/\x{111}/d/g;	# d with stroke
  return $term;
}

sub field_match {
  my $field1 = shift;
  my $field2 = shift;
  $field1 eq $field2 and return 1;
  return 0;
}

sub field_match_word {
  my $term_1 = shift;
  my $term_2 = shift;
  my $match_word_limit = shift;
  # return true if all words in $t_search are found in $t_record
  my @list_1 = split(/\s+/, $term_1);
  my @list_2 = split(/\s+/, $term_2);
  my $l1 = scalar @list_1;
  my $l2 = scalar @list_2;
  #my $l = $l1 >= $l2 ? $l1 : $l2;
  my $l = $l1;
  #$l1 < $match_word_limit and $match_word_limit = $l1;
  $l < $match_word_limit and $match_word_limit = $l;
  #$debug and print DEBUG join(",", $match_word_limit, $l1, $l2, $term_1, $term_2), "\n";
  my $match_count = 0;
  #for (my $i = 0; $i < $match_word_limit; $i++) {
  for (my $i = 0; $i < $l; $i++) {
    v($list_1[$i]) eq v($list_2[$i]) and $match_count++;
  }
  $match_count >= $match_word_limit and return 'yes';
  $match_count > $l/2 and return 'near';
  return 'no';
} 

sub getRecFmt {
  my $record = shift;
  my $leader = $record->leader() or return '';
  my $recTyp = substr($leader, 6, 1);
  my $bibLev = substr($leader, 7, 1);
  $recTyp eq 'z' and return "AU";
  $recTyp =~ /[abcdefgijkmoprt]/ or do {
    print STDERR "invalid recTyp: $recTyp\n";
    return '';
  };
  $bibLev =~ /[abcdms]/ or do {
    print STDERR "invalid bibLev: $bibLev\n";
    return '';
  };
  $recTyp =~ /[at]/ and $bibLev =~ /[acdm]/ and return "BK";
  $recTyp =~ /[m]/ and $bibLev =~ /[abcdms]/ and return "CF";
  $recTyp =~ /[gkor]/ and $bibLev =~ /[abcdms]/ and return "VM";
  $recTyp =~ /[cdij]/ and $bibLev =~ /[abcdms]/ and return "MU";
  $recTyp =~ /[ef]/ and $bibLev =~ /[abcdms]/ and return "MP";
  $recTyp =~ /[a]/ and $bibLev =~ /[bs]/ and return "SE";
  $recTyp =~ /[bp]/ and $bibLev =~ /[abcdms]/ and return "MX";
  # no match  --error
  die "can't set bib fmt, recTyp=$recTyp, bibLev=$bibLev\n";
}

sub Create_conn_to_Zserver {
  my $tconn;
  $tconn  = new ZOOM::Connection('zcat.oclc.org', 210, databaseName => 'OLUCWorldCat',
    #elementSetName => "F",
#    elementSetName => "FA", 	# full holdings
    elementSetName => "FD", 	# default holdings
    preferredRecordSyntax => "usmarc",
    myName => "OCLC",
    smallSetUpperBound => 0,
    largeSetLowerBound => 1,
    mediumSetPresentNumber => 0,
    user => "$user",
    pass => "$pass",
    charset => "utf-8",
    ) or die "can't connect to oclc server: $!\n";
  return $tconn;
}

sub parse_line {
  my $line = shift;
  my $in = {};
  foreach my $field (split("\t", $line)) {
    my ($parm, $value) = split(/:/, $field, 2);
    $parm or do {
      print "parse error parm: '$parm', value: '$value' ($field)\n";
      next;
    };
    $in->{$parm} = $value;
  }
  return $in;
}

sub unparse_line {
  my $hash = shift;
  my $list = [];
  foreach my $key (sort keys %$hash) {
    push @$list, "$key:$hash->{$key}";
  }
  return join("\t", @$list);
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

sub getDate {
  my $inputDate = shift;
  if (!defined($inputDate)) { $inputDate = time; }
  my ($ss,$mm,$hh,$day,$mon,$yr,$wday,$yday,$isdst) = localtime($inputDate);
  my $year = $yr + 1900;
  $mon++;
  my $fmtdate = sprintf("%4.4d%2.2d%2.2d",$year,$mon,$day);
  return $fmtdate;
}

sub get_pub_date {
  my $bib = shift;
  my $date_source;
  my $pub_date;
  my $f008 = $bib->field('008')->as_string();

  $pub_date = get_bib_data($bib, "260", 'c') and $date_source = '260';
  $pub_date or ($pub_date = get_bib_data($bib, "264#1", 'c') and $date_source = '264');
  $pub_date or ($pub_date = substr($f008, 7, 4) and $date_source = '008');
  
  $pub_date or $date_source = 'none';
  return ($pub_date, $date_source);
}
 
sub get_bib_data {
  my $bib = shift;
  my $tag = shift;
  my $i1 = '';
  my $i2 = '';
  length($tag) > 3 and do {
    length($tag) >= 4 and $i1 = substr($tag,3,1);
    length($tag) >= 5 and $i2 = substr($tag,4,1);
    $tag = substr($tag,0,3);
  };
  my $subfields = shift;
  my $data = [];
  my $field_string;
  TAG:foreach my $field ( $bib->field($tag) )  {
    $i1 ne '' and $i1 ne '#' and $field->indicator(1) != $i1 and next TAG;
    $i2 ne '' and $i2 ne '#' and $field->indicator(2) != $i2 and next TAG;
    $field_string = $field->as_string("'$subfields'") and push @$data, $field_string;
  }
  my $string = join(",", @$data);
  $string =~ s/^\s*(.*?)\s*$/$1/;    # trim leading and trailing whitespace
  return $string;
}

sub print_report_header {
  my $fh = shift;
  print $fh join("\t", 
    "Notes",			# a
    "Number",			# b
    "Arrival date",		# c
    "EYM",			# d
    "Alma MMSID",		# e
    "record source",		# f
    "OCLC number",		# g
    "Title",			# h
    "Author",			# i
    "Alma pub date",		# j
    "OCLC pub date",		# k
    "008 Language",		# l
    "Recfmt",			# m
    "has_245h",			# n
    "DLC",			# o
    "PCC",			# p
    "OCLC Elvl",		# q
    "OCLC 1st 050",		# r
    "040 b",			# s
    "# of Institutions",	# t
    "Institutions",		# u
    "match type",		# v
    "searched on",		# w
    "title match",		# x
    "author match",		# y
    "date match",		# z
    "Big10 OCLC inst code",	# aa
    "OCLC numbered series",	# ab
    "Alma fund",		# ac
    "Alma vendor",		# ad
    "OCLC country code",	# ae
    "OCLC geo code",		# af
  ), "\n";
}

sub print_near_match {
  my $fh = shift;
  my $rec_info = shift;
  my $alma_info = shift;
  my $info_list = $rec_info->{info_entries};
  print_alma_info($RPT_NEAR_MATCH, $alma_info, $rec_info, 'near match');
  foreach my $info_entry (@$info_list) {
    $info_entry->{match_type} =~ /^near/ or next;
    print_match_info($fh, $info_entry, $alma_info, $rec_info, 'near_match');
  }
  print $fh "'-------------------------------\n";
}

sub print_match {
  my $fh = shift;
  my $rec_info = shift;
  my $alma_info = shift;
  my $info_list = $rec_info->{info_entries};
  my $pm_lines = 0;
  foreach my $info_entry (@$info_list) {
    $info_entry->{match_type} =~ /^match/ or do {
      print "match type $info_entry->{match_type}\n";
      next;
    };
    $pm_lines++;
    print_match_info($fh, $info_entry, $alma_info, $rec_info, 'match');
  }
  return $pm_lines;
}

sub print_match_info {
  my $fh = shift;
  my $info_entry = shift;
  my $alma_info = shift;
  my $rec_info = shift;
  my $print_type = shift;

  my $eym_value = '';
  if ($print_type eq 'match') {
    foreach my $eym_cnt ( split( /,/, v($rec_info->{eym_count_list})) ) {
      $eym_cnt > 0 and $eym_value = 1;
    }
  } elsif ( $print_type eq 'near_match') {
    $eym_value = v($info_entry->{eym_cnt});
  }

  my $num_of_inst = 0;
  my $inst_list = '';
  defined $info_entry->{institutions} and do {
    $num_of_inst = scalar(@{$info_entry->{institutions}}),	# "# of Institutions",
    $inst_list = join("; ", @{$info_entry->{institutions}}),	# "Institutions",
  };
  my $big_ten_inst_list = join(", ", @{$info_entry->{big_ten_institutions}});

  print $fh join("\t", 
    "",						# "Notes",
    "",						# "Number",
    "'" . v($alma_info->{ARRIVAL_DATE}), 	# "Arrival date",
    $eym_value = v($info_entry->{eym_cnt}),     # "EYM",
    "'" . v($alma_info->{SYSNUM}), 		# "Alma sysnum",
    "OCLC",					# "record source",
    "'" . v($info_entry->{rec_oclc_num}),	# "OCLC number",
    v($info_entry->{rec_title}),		# "Title",
    v($info_entry->{rec_author}),		# "Author",
    "'" . v($alma_info->{DATE}),		# "Alma pub date",
    "'" . v($info_entry->{rec_pub_date}), 	# "OCLC pub date",
    v($info_entry->{rec_lang}),			# "008 Language",
    v($info_entry->{rec_fmt}),			# "Recfmt",
    v($info_entry->{has_245h}),			# "has_245h",
    v($info_entry->{dlc}),			# "DLC",
    v($info_entry->{pcc}),			# "PCC",
    v($info_entry->{elvl}),			# "OCLC Elvl",
    v($info_entry->{f050}),			# "OCLC 1st 050",
    v($info_entry->{rec_040_b}),		# "040 b",
    $num_of_inst,				# "# of Institutions",
    $inst_list,					# "Institutions",
    v($info_entry->{match_type}),		# "match type",
    v($info_entry->{search}), 			# "searched on",
    v($info_entry->{title_match}), 		# "title match",
    v($info_entry->{author_match}), 		# "author match",
    v($info_entry->{date_match}), 		# "date match",
    $big_ten_inst_list,				# "Big10 OCLC inst code",
    v($info_entry->{oclc_numbered_series}),	# "OCLC numbered series",
    v($alma_info->{FUND}),			# "Alma fund",
    v($alma_info->{VENDOR}),			# "Alma vendor",
    v($info_entry->{oclc_country_code}),	# "OCLC country code",
    v($info_entry->{oclc_geo_code}),		# "OCLC geo code",
    ), "\n";
}

sub print_alma_info {
  my $fh = shift;
  my $alma_info = shift;
  my $rec_info = shift;
  my $match_type = shift;
  print $fh join("\t", 
    "", 				# "Notes",
    "",					# "Number",
    "'" . v($alma_info->{ARRIVAL_DATE}),	# "Arrival date",
    v($rec_info->{eym_count_list}),	# "EYM",
    "'" . v($alma_info->{SYSNUM}),	# "Alma sysnum",
    "ALMA",				# "record source",
    "",					# "OCLC number",
    v($alma_info->{TITLE}),		# "Title",
    v($alma_info->{AUTHOR}),		# "Author",
    "'" . v($alma_info->{DATE}),		# "Alma pub date",
    "",					# "OCLC pub date",
    v($alma_info->{LANG}),		# "008 Language",
    v($alma_info->{FMT}),			# "Recfmt",
    v($alma_info->{HAS_245H}),		# "has_245h",
    "",					# "DLC",
    "",					# "PCC",
    "",					# "OCLC Elvl",
    "",					# "OCLC 1st 050",
    "",					# "040 b",
    v($rec_info->{inst_count_list}),	# "# of Institutions",
    "",					# "Institutions",
    $match_type,			# "match type",
    "",					# "searched on",
    "",					# "title match",
    "",					# "author match",
    "",					# "date match",
    "",					# "Big10 OCLC inst code",
    "",					# "OCLC numbered series",
    v($alma_info->{FUND}),		# "Alma fund (not used)",
    v($alma_info->{VENDOR}),		# "Alma vendor (not used)",
    "",					# "OCLC country code",
    "",					# "OCLC geo code",
    ), "\n";
}

sub set_match_type {
  my $info = shift;
  my $info_array = [];
  $info->{match_type} = '';
  # $info->{search}, $info->{title_match}, $info->{author_match}, $info->{date_match};
  foreach my $match_entry (@match_type) {
    my ($search, $tm, $am, $dm, $type) = @$match_entry;
    #print STDERR "match entry: ", join(",", $search, $tm, $am, $dm, $type), "\n";
    ($search eq '' or $search eq $info->{search}) and $tm eq $info->{title_match} and $am eq $info->{author_match} and $dm eq $info->{date_match} and return $type;
  }
  print STDERR "$sysnum: no type found in match_type, ", join(", ", 
    $info->{search}, 
    $info->{title_match}, 
    $info->{author_match}, 
    $info->{date_match}, 
  ), "\n";
  return 'exclude';
}

sub process_institutions {
  my $mrec = shift;

  my $all_institutions = [];
  my $big_ten_institutions = [];
  my $eym_cnt = 0;
  #948    |aUS|bIL|cUIU|dUNIV OF ILLINOIS
  foreach my $f948 ($mrec->field('948')) {
    my $inst = $f948->as_string('d');
    my $symbol = $f948->as_string('c');
    #$inst and push @$institutions, $inst;
    $symbol and push @$all_institutions, $symbol;
    $symbol eq 'EYM' and $eym_cnt++;
    $big_ten_institutions{$symbol} and push @$big_ten_institutions, $symbol;
  }
  return($all_institutions, $eym_cnt, $big_ten_institutions);
}

sub open_fh {
  my $filename = shift;
  my $mode = shift;
  my $fh = FileHandle->new($filename, $mode) or die "error opening file $filename, mode $mode\n";
  binmode $fh, 'utf8';
  return $fh;
}

sub v {
  my $value = shift;
  defined $value and return $value;
  return '';
}
