package Dongry::Query;
use strict;
use warnings;
our $VERSION = '1.0';

push our @CARP_NOT, qw(Dongry::Database);

sub new ($%) {
  my $class = shift;
  return bless {@_}, $class;
} # new

sub clone ($) {
  my $class = ref $_[0];
  return bless {%{$_[0]}}, $class;
} # clone

sub is_null ($) {
  return not defined $_[0]->{table_name};
} # is_null

sub db { if (@_ > 1) { $_[0]->{db} = $_[1] } return $_[0]->{db} }

sub table_name {
  if (@_ > 1) { $_[0]->{table_name} = $_[1] } return $_[0]->{table_name};
} # table_name

sub table {
  return $_[0]->{table} ||= $_[0]->db->table ($_[0]->table_name);
} # table

sub fields {
  if (@_ > 1) { $_[0]->{fields} = $_[1] } return $_[0]->{fields};
} # fields

sub where {
  if (@_ > 1) { $_[0]->{where} = $_[1] } return $_[0]->{where};
} # where

sub order {
  if (@_ > 1) { $_[0]->{order} = $_[1] } return $_[0]->{order};
} # order

sub group {
  if (@_ > 1) { $_[0]->{group} = $_[1] } return $_[0]->{group};
} # group

sub source_name {
  if (@_ > 1) { $_[0]->{source_name} = $_[1] } return $_[0]->{source_name};
} # source_name

sub lock {
  if (@_ > 1) { $_[0]->{lock} = $_[1] } return $_[0]->{lock};
} # lock

sub item_list_filter {
  my $self = $_[0];
  if ($self->{item_list_filter}) {
    return scalar $self->{item_list_filter}->(@_);
  } else {
    return $_[1];
  }
} # item_list_filter

sub find_all {
  my ($self, %args) = @_;
  return List::Rubyish->new if $self->is_null;
  return $self->item_list_filter
      ($self->table->find_all
           ($self->where,
            fields => $self->fields,
            group => $self->group,
            order => $self->order,
            offset => $args{offset},
            limit => $args{limit},
            source_name => $args{source_name} || $self->source_name,
            lock => $args{lock} || $self->lock));
} # find_all

sub find {
  my ($self, %args) = @_;
  return undef if $self->is_null;
  return $self->item_list_filter
      ($self->table->find_all
           ($self->where,
            fields => $self->fields,
            group => $self->group,
            order => $self->order,
            offset => 0,
            limit => 1,
            source_name => $args{source_name} || $self->source_name,
            lock => $args{lock} || $self->lock))->[0]; # or undef
} # find

sub count {
  my ($self, %args) = @_;
  return 0 if $self->is_null;

  my %param;
  my $group = $self->group;
  if ($group) {
    $param{fields} = {-count => $group, distinct => 1, as => 'count'};
  } else {
    $param{fields} = {-count => undef, as => 'count'};
  }

  my $row = $self->table->find
      ($self->where,
       %param,
       source_name => $args{source_name} || $self->source_name,
       lock => $args{lock} || $self->lock);
  return $row ? $row->get ('count') : 0;
} # count

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
