package Oracle::DML::Common;

# Perl standard modules
use strict;
use warnings;
use Carp;
# use CGI::Carp qw(fatalsToBrowser warningsToBrowser);
# warningsToBrowser(1);
# use CGI;
# use Getopt::Std;
use Debug::EchoMessage;
use DBI;

our $VERSION = 0.2;

require Exporter;
our @ISA         = qw(Exporter);
our @EXPORT      = qw();
our @EXPORT_OK   = qw(get_dbh is_object_exist 
    get_table_definition 
  );
our %EXPORT_TAGS = (
    all    =>[@EXPORT_OK],
    db_conn=>[qw(get_dbh is_object_exist
             )],
    table  =>[qw(get_table_definition
             )],
);

=head1 NAME

Oracle::DML::Common - Perl class for creating Oracle triggers

=head1 SYNOPSIS

  use Oracle::DML::Common;

  my %cfg = ('conn_string'=>'usr/pwd@db', 'table_name'=>'my_ora_tab');
  my $ot = Oracle::DML::Common->new;
  # or combine the two together
  my $ot = Oracle::DML::Common->new(%cfg);
  my $sql= $ot->prepare(%cfg); 
  $ot->execute();    # actually create the audit table and trigger


=head1 DESCRIPTION

This class contains methods to create audit tables and triggers for
Oracle tables.

=cut

=head3 new ()

Input variables:

  %ha  - any hash array containing initial parameters

Variables used or routines called:

  None

How to use:

   my $obj = new Oracle::DML::Common;      # or
   my $obj = Oracle::DML::Common->new;  

Return: new empty or initialized Oracle::DML::Common object.

This method constructs a Perl object and capture any parameters if
specified. 
 
=cut

sub new {
    my $caller        = shift;
    my $caller_is_obj = ref($caller);
    my $class         = $caller_is_obj || $caller;
    my $self          = bless {}, $class;
    my %arg           = @_;   # convert rest of inputs into hash array
    foreach my $k ( keys %arg ) {
        if ($caller_is_obj) {
            $self->{$k} = $caller->{$k};
        } else {
            $self->{$k} = $arg{$k};
        }
    }
    return $self;
}

=head1 METHODS

The following are the common methods, routines, and functions used
by other classes.

=head2 Connection Methods

The I<:db_conn> tag includes sub-rountines for creating and
managing database connections.

  use Oracle::DML::Common qw(:db_conn);

It includes the following sub-routines:

=head3 get_dbh($con, $dtp)

Input variables:

  $con - Connection string for
         Oralce: usr/pwd@db (default)
            CSV: /path/to/file
       ODBC|SQL: usr/pwd@DSN[:approle/rolepwd]
  $dtp - Database type: Oracle, CSV, etc

Variables used or routines called:

  DBI
  DBD::Oracle
  Win32::ODBC

How to use:

  $self->get_dbh('usr/pwd@dblk', 'Oracle');
  $self->get_dbh('usr/pwd@dblk:approle/rpwd', 'SQL');

Return: database handler

If application role is provided, it will activate the application role
as well.

=cut

sub get_dbh {
    my $self = shift;
    my ($con, $dtp) = @_;
    # Input variables:
    #   $con  - connection string: usr/pwd@db
    #   $dtp  - database type: Oracle, CSV
    #
    $dtp = 'Oracle' if !$dtp;
    my (@conn, $dsn, $dbh,$msg);
    my ($usr, $pwd, $sid) = ($con =~ /(\w+)\/(\w+)\@(\w+)/i);
    my ($apusr, $appwd) = ($con =~ /:(\w+)\/(\w+)/i);
    if ($dtp =~ /Oracle/i) {
        @conn = ("DBI:Oracle:$sid", $usr, $pwd);
        $dbh=DBI->connect(@conn) ||
            die "Connection error : $DBI::errstr\n";
        $dbh->{RaiseError} = 1;
    } elsif ($dtp =~ /CSV/i) {
        carp "WARN: CSV directory - $con does not exist.\n"
            if (!-d $con);
        @conn = ("DBI:CSV:f_dir=$con","","");
        $dbh=DBI->connect(@conn) ||
            die "Connection error : $DBI::errstr\n";

    } else {   # ODBC or SQL
        $dsn = "DSN=$sid;uid=$usr;pwd=$pwd;";
        $dbh = new Win32::ODBC($dsn);
        if (! $dbh) {
            Win32::ODBC::DumpError();
            $msg = "Could not open connection to DSN ($dsn) ";
            $msg .= "because of [$!]";
            die "$msg";
        }
        if ($apusr) {
            $dbh->Sql("exec sp_setapprole $apusr, $appwd");
        }
    }
    return $dbh;
}

=head3 is_object_exist($dbh,$tn,$tp)

Input variables:

  $dbh - database handler, required.
  $tn  - table/object name, required.
         schema.table_name is allowed.

Variables used or routines called:

  echoMSG    - display messages.

How to use:

  # whether table 'emp' exist
  $yesno = $self->is_object_exist($dbh,'emp');

Return: 0 - the object does not exist;
        1 - the object exist;

=cut

sub is_object_exist {
    my $self = shift;
    my($dbh,$tn, $tp) = @_;
    croak "ERR: could not find database handler.\n"      if !$dbh;
    croak "ERR: no table or object name is specified.\n" if !$tn;
    # get owner name and table name
    my ($sch, $tab, $stb) = ("","","");
    if (index($tn, '.')>0) {
        ($sch, $tab) = ($tn =~ /(\w+)\.([\w\$]+)/);
    }
    my($q,$r);
    $tp = 'TABLE' if ! $tp;
    $stb = 'user_objects';
    $stb = 'all_objects'   if $sch;
    $q  = "SELECT object_name from $stb ";
    $q .= " WHERE object_type = '" . uc($tp) . "'";
    if ($sch) {
        $q .= "   AND object_name = '" . uc($tab) . "'";
        $q .= "   AND owner = '" . uc($sch) . "'";
    } else {
        # $tn =~ s/\$/\\\$/g;
        $q .= "   AND object_name = '" . uc($tn) . "'";
    }
    $self->echoMSG($q, 5);
    my $sth=$dbh->prepare($q) || die  "Stmt error: $dbh->errstr";
       $sth->execute() || die "Stmt error: $dbh->errstr";
    my $n = $sth->rows;
    my $arf = $sth->fetchall_arrayref;
    $r = 0;
    $r = 1             if ($#{$arf}>=0);
    return $r;
}

=head2 Table Methods

The I<:table> tag includes sub-rountines for creating, checking and
manipulating tables.

  use Oracle::DML::Common qw(:table);

It includes the following sub-routines:

=head3 get_table_definition($dbh,$tn,$cns,$otp)

Input variables:

  $dbh - database handler, required.
  $tn  - table/object name, required.
         schema.table_name is allowed.
  $cns - column names separated by comma.
         Default is null, i.e., to get all the columns.
         If specified, only get definition for those specified.
  $otp - output array type:
         AR|ARRAY        - returns ($cns,$df1,$cmt)
         AH1|ARRAY_HASH1 - returns ($cns,$df2,$cmt)
         HH|HASH         - returns ($cns,$df3,$cmt)
         AH2|ARRAY_HASH2 - returns ($cns,$df4,$cmt)

Variables used or routines called:

  echoMSG - display messages.

How to use:

  ($cns,$df1,$cmt) = $self->getTableDef($dbh,$table_name,'','array');
  ($cns,$df2,$cmt) = $self->getTableDef($dbh,$table_name,'','ah1');
  ($cns,$df3,$cmt) = $self->getTableDef($dbh,$table_name,'','hash');
  ($cns,$df4,$cmt) = $self->getTableDef($dbh,$table_name,'','ah2');

Return:

  $cns - a list of column names separated by comma.
  $df1 - column definiton array ref in [$seq][$cnn].
    where $seq is column sequence number, $cnn is array
    index number corresponding to column names: 
          0 - cname, 
          1 - coltype, 
          2 - width, 
          3 - scale, 
          4 - precision, 
          5 - nulls, 
          6 - colno,
          7 - character_set_name.
  $df2 - column definiton array ref in [$seq]{$itm}.
    where $seq is column number (colno) and $itm are:
          col - column name
          seq - column sequence number
          typ - column data type
          wid - column width
          max - max width
          min - min width
          dec - number of decimals
          req - requirement: null or not null
          dft - date format
          dsp - description or comments
  $df3 - {$cn}{$itm} when $otp = 'HASH'
    where $cn is column name in lower case and
          $itm are the same as the above
  $df4 - [$seq]{$itm} when $otp = 'AH2'
    where $seq is the column number, and $itm are:
          cname     - column name (col)
          coltype   - column data type (typ)
          width     - column width (wid)
          scale     - column scale (dec)
          precision - column precision (wid for N)
          nulls     - null or not null (req)
          colno     - column sequence number (seq)
          character_set_name - character set name

=cut

sub get_table_definition {
    my $self = shift;
    my($dbh, $tn, $cns, $otp) = @_;
    # Input variables:
    #   $dbh - database handler
    #   $tn  - table name
    #   $cns - column names
    #
    # 0. check inputs
    croak "ERR: could not find database handler.\n" if !$dbh;
    croak "ERR: no table or object name is specified.\n" if !$tn;
    $tn = uc($tn);
    $self->echoMSG("  - reading table $tn definition...", 1);
    $otp = 'ARRAY' if (! defined($otp));
    $otp = uc $otp;
    if ($cns) { $cns =~ s/,\s*/','/g; $cns = "'$cns'"; }
    #
    # 1. retrieve column definitions
    my($q,$msg);
    if (index($tn,'.')>0) {   # it is in schema.table format
        my ($sch,$tab) = ($tn =~ /([-\w]+)\.([-\w]+)/);
        $q  = "  SELECT column_name,data_type,data_length,";
        $q .= "data_scale,data_precision,\n             ";
        $q .= "nullable,column_id,character_set_name\n";
        $msg = "$q";
        $q   .= "        FROM dba_tab_columns\n";
        $msg .= "        FROM dba_tab_columns\n";
        $q   .= "       WHERE owner = '$sch' AND table_name = '$tab'\n";
        $msg .= "       WHERE owner = '$sch' AND table_name = '$tab'\n";
    } else {
        $q  = "  SELECT cname,coltype,width,scale,precision,nulls,";
        $q .= "colno,character_set_name\n";
        $msg = "$q";
        $q   .= "        FROM col\n     WHERE tname = '$tn'";
        $msg .= "        FROM col\n     WHERE tname = '$tn'\n";
    }
    if ($cns) {
        $q   .= "         AND cname in (" . uc($cns) . ")\n";
        $msg .= "         AND cname in (" . uc($cns) . ")\n";
    }
    if (index($tn,'.')>0) {   # it is in schema.table format
        $q   .= "\n    ORDER BY table_name,column_id";
        $msg .= "    ORDER BY table_name, column_id\n";
    } else {
        $q   .= "\n    ORDER BY tname, colno";
        $msg .= "    ORDER BY tname, colno\n";
    }
    $self->echoMSG("    $msg", 2);
    my $sth=$dbh->prepare($q) || croak "ERR: Stmt - $dbh->errstr";
       $sth->execute() || croak "ERR: Stmt - $dbh->errstr";
    my $arf = $sth->fetchall_arrayref;       # = output $df1
    #
    # 2. construct column name list
    my $r = ${$arf}[0][0];
    for my $i (1..$#{$arf}) { $r .= ",${$arf}[$i][0]"; }
    $msg = $r; $msg =~ s/,/, /g;
    $self->echoMSG("    $msg", 5);
    #
    # 3. get column comments
    $q  = "SELECT column_name, comments\n      FROM user_col_comments";
    $q .= "\n     WHERE table_name = '$tn'";
    $msg  = "SELECT column_name, comments\nFROM user_col_comments";
    $msg .= "\nWHERE table_name = '$tn'<p>";
    $self->echoMSG("    $msg", 5);
    my $s2=$dbh->prepare($q) || croak "ERR: Stmt - $dbh->errstr";
       $s2->execute() || croak "ERR: Stmt - $dbh->errstr";
    my $brf = $s2->fetchall_arrayref;
    my (%cmt, $j, $k, $cn);
    for my $i (0..$#{$brf}) {
        $j = lc(${$brf}[$i][0]);             # column name
        $cmt{$j} = ${$brf}[$i][1];           # comments
    }
    #
    # 4. construct output $df2($def) and $df3($df2)
    my $def = bless [], ref($self)||$self;   # = output $df2
    my $df2 = bless {}, ref($self)||$self;   # = output $df3
    for my $i (0..$#{$arf}) {
        $j  = ${$arf}[$i][6]-1;              # column seq number
        ${$def}[$j]{seq} = $j;               # column seq number
        $cn = lc(${$arf}[$i][0]);            # column name
        ${$def}[$j]{col} = uc($cn);          # column name
        ${$def}[$j]{typ} = ${$arf}[$i][1];   # column type
        if (${$arf}[$i][4]) {                # precision > 0
            # it is NUMBER data type
            ${$def}[$j]{wid} = ${$arf}[$i][4];  # column width
            ${$def}[$j]{dec} = ${$arf}[$i][3];  # number decimal
        } else {                             # CHAR or VARCHAR2
            ${$def}[$j]{wid} = ${$arf}[$i][2];  # column width
            ${$def}[$j]{dec} = ""               # number decimal
        }
        ${$def}[$j]{max} = ${$def}[$j]{wid};

        if (${$def}[$j]{typ} =~ /date/i) {   # typ is DATE
            ${$def}[$j]{max} = 17;           # set width to 17
            ${$def}[$j]{wid} = 17;           # set width to 17
            ${$def}[$j]{dft} = 'YYYYMMDD.HH24MISS';
        } else {
            ${$def}[$j]{dft} = '';           # set date format to null
        }
        if (${$arf}[$i][5] =~ /^(not null|N)/i) {
            ${$def}[$j]{req} = 'NOT NULL';
        } else {
            ${$def}[$j]{req} = '';
        }
        if (exists $cmt{$cn}) {
            ${$def}[$j]{dsp} =  $cmt{$cn};
        } else {
            ${$def}[$j]{dsp} = '';
        }
        ${$def}[$j]{min} = 0;
        ${$df2}{$cn}{seq}  = $j;
        ${$df2}{$cn}{col}  = ${$def}[$j]{col};
        ${$df2}{$cn}{typ}  = ${$def}[$j]{typ};
        ${$df2}{$cn}{dft}  = ${$def}[$j]{dft};
        ${$df2}{$cn}{wid}  = ${$def}[$j]{wid};
        ${$df2}{$cn}{dec}  = ${$def}[$j]{dec};
        ${$df2}{$cn}{max}  = ${$def}[$j]{max};
        ${$df2}{$cn}{min}  = ${$def}[$j]{min};
        ${$df2}{$cn}{req}  = ${$def}[$j]{req};
        ${$df2}{$cn}{dsp}  = ${$def}[$j]{dsp};
    }
    #
    # 5. construct output array $df4
    my $df4 = bless [],ref($self)||$self;   # = output $df4
    for my $i (0..$#{$arf}) {
        $j = lc(${$arf}[$i][0]);            # column name
        push @$df4, {cname=>$j,         coltype=>${$arf}[$i][1],
                width=>${$arf}[$i][2],    scale=>${$arf}[$i][3],
            precision=>${$arf}[$i][4],    nulls=>${$arf}[$i][5],
                colno=>${$arf}[$i][6],
            character_set_name=>${$arf}[$i][7]};
    }
    #
    # 6. output based on output type
    if ($otp =~ /^(AR|ARRAY)$/i) {
        return ($r, $arf, \%cmt);      # output ($cns,$df1,$cmt)
    } elsif ($otp =~ /^(AH1|ARRAY_HASH1)$/i) {
        return ($r, $def, \%cmt);      # output ($cns,$df2,$cmt)
    } elsif ($otp =~ /^(HH|HASH)$/i) {
        return ($r, $df2, \%cmt);      # output ($cns,$df3,$cmt)
    } else {
        return ($r, $df4, \%cmt);      # output ($cns,$df4,$cmt);
    }
}

1;

=head1 HISTORY

=over 4

=item * Version 0.1

This versionwas contained in Oracle::Trigger class.

=item * Version 0.2

04/29/2005 (htu) - extracted common routines from Oracle::Trigger class
and formed Oracle::DML::Common.

=cut

=head1 SEE ALSO (some of docs that I check often)

Data::Describe, Oracle::Loader, CGI::Getopt, File::Xcopy,
Oracle::Trigger,
perltoot(1), perlobj(1), perlbot(1), perlsub(1), perldata(1),
perlsub(1), perlmod(1), perlmodlib(1), perlref(1), perlreftut(1).

=head1 AUTHOR

Copyright (c) 2005 Hanming Tu.  All rights reserved.

This package is free software and is provided "as is" without express
or implied warranty.  It may be used, redistributed and/or modified
under the terms of the Perl Artistic License (see
http://www.perl.com/perl/misc/Artistic.html)

=cut


