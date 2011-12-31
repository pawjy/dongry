package Dongry::SQL;
use strict;
use warnings;
our $VERSION = '1.0';
use Carp;
use Exporter::Lite;

our @EXPORT;

our $SortKeys;

## <http://dev.mysql.com/doc/refman/5.6/en/identifiers.html>.
push @EXPORT, qw(quote);
sub quote ($) {
  my $s = $_[0];
  $s =~ s/`/``/g;
  return q<`> . $s . q<`>;
} # quote

## <http://dev.mysql.com/doc/refman/5.6/en/string-literals.html>.
push @EXPORT, qw(like);
sub like ($) {
  my $s = $_[0];
  $s =~ s/([\\%_])/\\$1/g;
  return $s;
} # like

push @EXPORT, qw(fields);
sub fields ($);
sub fields ($) {
  if (not defined $_[0]) {
    return '*';
  } elsif (not ref $_[0]) {
    return quote $_[0];
  } elsif (ref $_[0] eq 'ARRAY') {
    if (@{$_[0]}) {
      return join ', ', map { fields ($_) } @{$_[0]};
    } else {
      croak 'Array reference cannot be empty';
    }
  } elsif (ref $_[0] eq 'HASH') {
    my $func = [grep { /^-/ } keys %{$_[0]}]->[0] || '';
    if ($func =~ /\A-(count|min|max|sum)\z/) {
      my $v = (uc $1) . '(';
      $v .= 'DISTINCT ' if $_[0]->{distinct};
      $v .= fields ($_[0]->{$func});
      $v .= ')';
      $v .= ' AS ' . quote $_[0]->{as} if defined $_[0]->{as};
      return $v;
    } else {
      if ($func) {
        croak sprintf 'Field function %s is not supported', $func;
      } else {
        croak 'Hash reference must contain a field function name';
      }
    }
  } elsif (ref $_[0] eq 'Dongry::SQL::BareFragment') {
    return ${$_[0]};
  } else {
    croak sprintf 'Field value %s is not supported', $_[0];
  }
} # fields

push @EXPORT, qw(where);
sub where ($;$);
sub where ($;$) {
  my ($values, $table_schema) = @_;
  if (ref $values eq 'HASH') {
    my @and;
    my @placeholder;
    for my $key (keys %$values) {
      my $sql = quote $key;
      my $type;
      my $op;
      my $value;
      if (defined $values->{$key} and ref $values->{$key} eq 'HASH') {
        $type = [grep { /^[-<>!=]/ } keys %{$values->{$key}}]->[0];
        $op = {
            -eq => '=',  '==' => '=',
            -ne => '!=', '!=' => '!=', -not => '!=',
            -lt => '<',  '<'  => '<',
            -le => '<=', '<=' => '<=',
            -gt => '>',  '>'  => '>',
            -ge => '>=', '>=' => '>=',
            -like => 'LIKE',
            -prefix => 'LIKE', -infix => 'LIKE', -suffix => 'LIKE',
            -regexp => 'REGEXP',
            -in => '-in',
        }->{$type || ''} or croak "No known operator is specified for |$key|";

        if ($op eq '-in') {
          my $list = $values->{$key}->{$type};
          croak "List for |-in| is empty" unless @{$list or []};
          $sql .= ' IN (' . (join ', ', ('?') x @$list) . ')';
          my $coltype = $table_schema->{type}->{$key};
          my $handler;
          if ($coltype) {
            $handler = $Dongry::Types->{$coltype}
                or croak "Type handler for |$coltype| is not defined";
          }
          push @placeholder, grep {
            croak "An undef is found in |-in| list" if not defined $_;
            croak "A reference is found in |-in| list" if ref $_;
            1;
          } map {
            if ($coltype) {
              $handler->{serialize}->($_);
            } else {
              $_;
            }
          } @$list;
        } else {
          $value = $values->{$key}->{$type};
        } # $op
      } else {
        $type = '-eq';
        $op = '=';
        $value = $values->{$key};
      } # $values->{$key}

      unless ($type eq '-in') {
        my $coltype = $table_schema->{type}->{$key};
        if ($coltype) {
          my $handler = $Dongry::Types->{$coltype}
              or croak "Type handler for |$coltype| is not defined";
          $value = $handler->{serialize}->($value);
        } else {
          if (defined $value and ref $value) {
            croak "Type for |$key| is not defined but a reference is specified";
          }
        }
        if (not defined $value) {
          if ($op eq '=') {
            $sql .= ' IS NULL';
          } elsif ($op eq '!=') {
            $sql .= ' IS NOT NULL';
          } else {
            croak "Operator |$type| does not allow an undef value";
          }
        } else {
          $sql .= ' ' . $op . ' ?';
          if ($type eq '-prefix') {
            push @placeholder, like ($value) . '%';
          } elsif ($type eq '-suffix') {
            push @placeholder, '%' . like ($value);
          } elsif ($type eq '-infix') {
            push @placeholder, '%' . like ($value) . '%';
          } else {
            push @placeholder, $value;
          }
        } # $value
      } # $type

      push @and, $sql;
    } # $values

    if (@and) {
      @and = sort { $a cmp $b } @and if $SortKeys;
      return ((join ' AND ', @and), \@placeholder);
    } else {
      croak "No condition is specified";
    }
  } elsif (ref $values eq 'ARRAY') {
    my ($sql, %bind) = @$values;
    croak 'No SQL template is specified'
        if not defined $sql or not length $sql;

    $sql =~ s{((`?)(\w+?)\2\s*(=|<=?|>=?|<>|!=|<=>)\s*)\?\s*}{$1:$3 }g;

    my %unused = map { $_ => 1 } keys %bind;
    my @placeholder;
    $sql =~ s{:(\w+)(?::(:?\w+))?}{
      my $key = $1;
      my $instruction = defined $2 ? $2 : '';
      my $column = $key;
      if (length $instruction and $instruction =~ s/^://) {
        $column = $instruction;
        undef $instruction;
      }

      my $type = ref ($bind{$key});
      delete $unused{$key};

      if ($instruction eq 'sub' or $instruction eq 'optsub') {
        croak "Value for |$key| is not defined" if not defined $bind{$key};
        croak "A non-reference value is specified for |$key|" unless $type;
        croak "A reference is specified for |$key|" if $type ne 'HASH';

        if (keys %{$bind{$key}}) {
          my ($sql, $bind) = where $bind{$key}, $table_schema;
          push @placeholder, @$bind;
          '(' . $sql . ')';
        } else {
          if ($instruction eq 'optsub') {
            '(1 = 1)';
          } else {
            croak "An empty hash reference is specified for |$key|";
          }
        }
      } elsif ($instruction eq 'id') {
        croak "Value for |$key| is not defined" if not defined $bind{$key};
        croak "A reference is specified for |$key|" if $type;
        quote $bind{$key};
      } elsif (length $instruction) {
        croak "Instruction |$instruction| is unknown";
      } elsif ($type eq 'ARRAY') {
        croak "List for |$key| is empty" unless @{$bind{$key} or []};
        my $coltype = $table_schema->{type}->{$column};
        my $handler;
        if ($coltype) {
          $handler = $Dongry::Types->{$coltype}
              or croak "Type handler for |$coltype| is not defined";
        }
        push @placeholder, grep { 
          croak "An undef is found in |$key| list" if not defined $_;
          croak "A reference is found in |$key| list" if ref $_;
          1;
        } map {
          if ($coltype) {
            $handler->{serialize}->($_);
          } else {
            $_;
          }
        } @{$bind{$key}};
        join ', ', map { '?' } @{$bind{$key}};
      } else {
        my $value = $bind{$key};
        my $coltype = $table_schema->{type}->{$column};
        if ($coltype) {
          my $handler = $Dongry::Types->{$coltype}
              or croak "Type handler for |$coltype| is not defined";
          $value = $handler->{serialize}->($value);
        } else {
          if (defined $value and ref $value) {
            croak "Type for |$column| is not defined but a reference is specified";
          }
        }
        croak "Value for |$key| is not defined" if not defined $value;
        push @placeholder, $value;
        '?';
      }
    }eg;

    if (keys %unused) {
      croak sprintf "Values %s are not used",
          join ', ', map { "|$_|" } keys %unused;
    }

    return ($sql, \@placeholder);
  } else {
    croak "Unknown where value |$values| is specified";
  }
} # where

push @EXPORT, qw(order);
sub order ($) {
  if (defined $_[0]) {
    my @s;
    for (0..(int (($#{$_[0] or []} + 2) / 2) - 1)) {
      push @s, (quote $_[0]->[$_ * 2]) . ' ' . (
        {
          'ASC' => 'ASC',
          'asc' => 'ASC',
          '1' => 'ASC',
          '+1' => 'ASC',
          'DESC' => 'DESC',
          'desc' => 'DESC',
          '-1' => 'DESC',
        }->{$_[0]->[$_ * 2 + 1] || 'ASC'} or
        (croak sprintf 'Unknown order: %s', $_[0]->[$_ * 2 + 1])
      );
    }
    return join ', ', @s;
  } else {
    return '';
  }
} # order

push @EXPORT, qw(reverse_order_struct);
sub reverse_order_struct ($) {
  if (defined $_[0]) {
    my @s = @{$_[0] or []};
    for my $i (0..(int (($#s + 2) / 2) - 1)) {
      $s[$i * 2 + 1] = {
        'ASC' => -1,
        'asc' => -1,
        '1' => -1,
        '+1' => -1,
        'DESC' => 1,
        'desc' => 1,
        '-1' => 1,
      }->{$s[$i * 2 + 1] || 1} or
      (croak sprintf 'Unknown order: %s', $_[0]->[$i * 2 + 1]);
    }
    return \@s;
  } else {
    return undef;
  }
} # reverse_order_struct

# ------ Bare SQL fragment ------

package Dongry::SQL::BareFragment;
our $VERSION = '1.0';

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
