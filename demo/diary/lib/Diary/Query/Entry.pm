package Diary::Query::Entry;
use strict;
use warnings;
use base qw(Dongry::Query);

sub db {
  require Dongry::Database;
  return Dongry::Database->load ('diary');
} # db

sub table_name {
  return 'entry';
} # table_name

sub item_list_filter {
  my ($self, $list) = @_;
  require Diary::Entry;
  $list = scalar $list->map (sub {
    return Diary::Entry->new_from_row ($_);
  });

  Dongry::Database->load ('user')->table ('user')->fill_related_rows
      ($list, {'author_id' => 'id'} => 'set_author_as_row');

  return $list;
} # item_list_filter

1;
