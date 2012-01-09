#!/usr/bin/perl
use strict;
use warnings;
use Path::Class;
use Getopt::Long;
use Pod::Usage;

my $dest_dir_name;
my $dest_package;
GetOptions (
  'dest-dir-name=s' => \$dest_dir_name,
  'dest-package=s' => \$dest_package,
) or pod2usage;
pod2usage unless $dest_dir_name;
pod2usage unless $dest_package;

my $dest_lib_dir_name = $dest_package;
$dest_lib_dir_name =~ s{::}{/}g;

my $dest_d = dir ($dest_dir_name);

my $root_d = file (__FILE__)->dir->parent;
chdir $root_d->stringify;

$root_d->recurse (callback => sub {
  my $f = shift;
  return if $f =~ m[(?:^|/)\.git/|~$];
  return unless -f $f;

  if ($f =~ m{
    ^lib/Dongry/
  }x) {
    my $file_name = $f->stringify;
    $file_name =~ s{^lib/}{}g;
    my $dest_f = $dest_d
        ->subdir ('lib', $dest_lib_dir_name)
        ->file ($file_name);
    warn "$f => $dest_f\n";

    my $data = $f->slurp;
    $data =~ s{Dongry::}{${dest_package}::Dongry::}g;
    $data =~ s{List::Ish}{${dest_package}::Dongry::List::Ish}g;
    
    $dest_f->dir->mkpath;
    my $dest_file = $dest_f->openw;
    print $dest_file $data;

  } elsif ($f eq 't/lib/Test/Dongry.pm') {
    my $dest_f = $dest_d
        ->subdir ('t', 'dongry', 'lib', 'Test', $dest_lib_dir_name)
        ->file ('Dongry.pm');

    warn "$f => $dest_f\n";

    my $data = $f->slurp;
    $data =~ s{Dongry::}{${dest_package}::Dongry::}g;
    $data =~ s{Test::Dongry}{Test::${dest_package}::Dongry}g;
    $data =~ s{List::Ish}{${dest_package}::Dongry::List::Ish}g;

    my $dest_lib_d = $dest_d->subdir ('lib')->relative ($dest_f->dir);
    $data =~ s{use lib .*?;}
        {use lib file (__FILE__)->dir->subdir (q<$dest_lib_d>)->stringify;};
    
    $dest_f->dir->mkpath;
    my $dest_file = $dest_f->openw;
    print $dest_file $data;

  } elsif ($f =~ m{
    ^t/lib/Test/Dongry
  }x) {
    my $file_name = $f->stringify;
    $file_name =~ s{^t/lib/Test/}{}g;
    my $dest_f = $dest_d
        ->subdir ('t', 'dongry', 'lib', 'Test', $dest_lib_dir_name)
        ->file ($file_name);

    warn "$f => $dest_f\n";

    my $data = $f->slurp;
    $data =~ s{Dongry::}{${dest_package}::Dongry::}g;
    $data =~ s{Test::Dongry}{Test::${dest_package}::Dongry}g;
    $data =~ s{List::Ish}{${dest_package}::Dongry::List::Ish}g;
    
    $dest_f->dir->mkpath;
    my $dest_file = $dest_f->openw;
    print $dest_file $data;

  } elsif ($f =~ m{
    ^t/
  }x) {
    my $file_name = $f->stringify;
    $file_name =~ s{^t/}{}g;
    my $dest_f = $dest_d->subdir ('t', 'dongry')->file ($file_name);

    warn "$f => $dest_f\n";

    my $data = $f->slurp;
    $data =~ s{Dongry::}{${dest_package}::Dongry::}g;
    $data =~ s{Test::Dongry}{Test::${dest_package}::Dongry}g;
    $data =~ s{List::Ish}{${dest_package}::Dongry::List::Ish}g;
    
    $dest_f->dir->mkpath;
    my $dest_file = $dest_f->openw;
    print $dest_file $data;
  }
});

for (
  ['modules/perl-ooutils/lib', 'List/Ish.pm'],
  ['modules/perl-ooutils/lib', 'List/Ish.pod'],
) {
  my $f = $root_d->subdir ($_->[0])->file ($_->[1]);
  my $dest_f = $dest_d
      ->subdir ('lib', $dest_lib_dir_name, 'Dongry')
      ->file ($_->[1]);

  warn "$f => $dest_f\n";

  my $data = $f->slurp;
  $data =~ s{List::Ish}{${dest_package}::Dongry::List::Ish}g;
  
  $dest_f->dir->mkpath;
  my $dest_file = $dest_f->openw;
  print $dest_file $data;
}

for (
  ['modules/perl-test-moremore/lib', 'Test/MoreMore.pm'],
  ['modules/perl-test-moremore/lib', 'Test/MoreMore/Mock.pm'],
  ['modules/perl-rdb-utils/lib', 'DBIx/ShowSQL.pm'],
  ['modules/perl-rdb-utils/lib', 'Test/MySQL/CreateDatabase.pm'],
  ['modules/perl-rdb-utils/lib', 'AnyEvent/DBI/Hashref.pm'],
  ['modules/perl-rdb-utils/lib', 'AnyEvent/DBI/Carp.pm'],
  ['modules/perl-json-functions-xs/lib', 'JSON/Functions/XS.pm'],
) {
  my $f = $root_d->subdir ($_->[0])->file ($_->[1]);
  my $dest_f = $dest_d->subdir ('t', 'dongry', 'lib')->file ($_->[1]);

  warn "$f => $dest_f\n";

  my $data = $f->slurp;
  
  $dest_f->dir->mkpath;
  my $dest_file = $dest_f->openw;
  print $dest_file $data;
}

=head1 NAME

bin/create-custom-copy.pl - Create a copy of Dongry in your favorite namespace

=head1 SYNOPSIS

  $ perl bin/create-custom-copy.pl \
    --dest-dir-name path/to/your/project \
    --dest-package Your::Package

=head1 DESCRIPTION

The C<bin/create-custom-copy.pl> script create a copy of Dongry
implementation and tests, in your favorite Perl namespace.  The
C<--dest-dir-name> option specifies the root directory of the copy.
The C<--dest-package> option specifies the package name prefix
prepended to the Dongry module names.  For example, if you specify
C<--dest-package> as C<Hoge::Fuga>, the L<Dongry::Database> module is
named as C<Hoge::Fuga::Dongry::Database> in your copy.

This script might be useful when it is desired for your application to
not depend on any external module, or when you write multiple modules
that depend on Dongry and you don't want to care whether the version
of Dongry required by a module is also compatible with the other
module.  However, nevertheless the existence of this script, such
usage is discouraged.

=head1 AUTHOR

Wakaba <w@suika.fam.cx>.

=head1 LICENSE

Copyright 2011-2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
