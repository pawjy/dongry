package Dongry::Query;
use strict;
use warnings;

push our @CARP_NOT, qw(Dongry::Database);

sub new {
  my $class = shift;
  return bless {@_}, $class;
} # new

sub db {
  if (@_ > 1) {
    $_[0]->{db} = $_[1];
  }
  return $_[0]->{db};
} # db

sub table_name {
  if (@_ > 1) {
    $_[0]->{table_name} = $_[1];
  }
  return $_[0]->{table_name};
} # table_name

sub table {
  return $_[0]->{table} ||= $_[0]->db->table ($_[0]->table_name);
} # table

sub where {
  if (@_ > 1) {
    $_[0]->{where} = $_[1];
  }
  return $_[0]->{where};
} # where

sub order {
  if (@_ > 1) {
    $_[0]->{order} = $_[1];
  }
  return $_[0]->{order};
} # order

sub item_list_filter {
  my $self = $_[0];
  if ($self->{item_list_filter}) {
    return $self->{item_list_filter}->(@_);
  } else {
    return $_[1]->map (sub { $self->item_filter ($_) });
  }
} # item_list_filter

sub item_filter {
  if ($_[0]->{item_filter}) {
    return $_[0]->{item_filter}->(@_);
  } else {
    return $_[1];
  }
} # item_filter

sub search {
  my ($self, %args) = @_;
  return List::Rubyish->new unless $self->table_name;
  return $self->item_list_filter
      ($self->db->select
           ($self->table_name,
            $self->where,
            order => $self->order,
            offset => $args{offset},
            limit => $args{limit},
            source_name => $args{source_name})->all_as_rows);
} # search

sub find {
  my ($self, %args) = @_;
  return $self->search (%args, offset => 0, limit => 1)->[0]; # or undef
} # find

sub count {
  my ($self, %args) = @_;
  return 0 unless $self->table_name;
  return $self->db->select
      ($self->table_name,
       $self->where,
       field => 'COUNT(*) AS count',
       source_name => $args{source_name})->first->{count} || 0;
} # count

1;