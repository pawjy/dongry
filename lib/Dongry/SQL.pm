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
our $NoAsInFields;
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
    if ($func =~ /\A-(count|min|max|sum|date)\z/) {
      my $v = (uc $1) . '(';
      $v .= 'DISTINCT ' if $_[0]->{distinct};
      {
        local $NoAsInFields = 1;
        $v .= fields ($_[0]->{$func});
      }
      if ($func eq '-date' and $_[0]->{delta}) {
        $v .= sprintf ' + INTERVAL %d SECOND', $_[0]->{delta};
      }
      $v .= ')';
      $v .= ' AS ' . quote $_[0]->{as}
          if defined $_[0]->{as} and not $NoAsInFields;
      return $v;
    } elsif ($func eq '-column') {
      my $v = quote $_[0]->{$func};
      $v .= ' AS ' . quote $_[0]->{as}
          if defined $_[0]->{as} and not $NoAsInFields;
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

sub _where_exp ($$$$$) {
  #my ($key, $type, $table_schema, $value, $placeholder) = @_;
  my $op = {
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
  }->{$_[1] || ''}
      or croak "Unknown operation |$_[1]| is specified for column |$_[0]|";

  my $coltype = $_[2]->{type}->{$_[0]};
  if ($coltype) {
    my $handler = $Dongry::Types->{$coltype}
        or croak "Type handler for |$coltype| is not defined";
    $_[3] = $handler->{serialize}->($_[3]);
  } else {
    if (defined $_[3] and ref $_[3]) {
      croak "Type for |$_[0]| is not defined but a reference is specified";
    }
  }

  if (not defined $_[3]) {
    if ($op eq '=') {
      return ((quote $_[0]) . ' IS NULL');
    } elsif ($op eq '!=') {
      return ((quote $_[0]) . ' IS NOT NULL');
    } else {
      croak "Operator |$_[1]| does not allow an undef value";
    }
  } else {
    if ($_[1] eq '-prefix') {
      push @{$_[4]}, like ($_[3]) . '%';
    } elsif ($_[1] eq '-suffix') {
      push @{$_[4]}, '%' . like ($_[3]);
    } elsif ($_[1] eq '-infix') {
      push @{$_[4]}, '%' . like ($_[3]) . '%';
    } else {
      push @{$_[4]}, $_[3];
    }
    return ((quote $_[0]) . ' ' . $op . ' ?');
  } # $_[3]
} # _where_exp

push @EXPORT, qw(where);
sub where ($;$);
sub where ($;$) {
  my ($values, $table_schema) = @_;
  if (ref $values eq 'HASH') {
    my @and;
    my @placeholder;
    my @key = keys %$values;
    @key = sort { $a cmp $b } @key if $SortKeys;
    for my $key (@key) {
      if (defined $values->{$key} and ref $values->{$key} eq 'HASH') {
        my @type = grep { /^[-<>!=]/ } keys %{$values->{$key}};
        croak "No operation is specified for column |$key|" unless @type;
        @type = sort { $a cmp $b } @type if $SortKeys;
        for my $type (@type) {
          if ($type eq '-in') {
            my $list = $values->{$key}->{$type};
            croak "List for |-in| is empty" unless @{$list or []};
            push @and, (quote $key) .
                ' IN (' . (join ', ', ('?') x @$list) . ')';
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
            push @and,
                _where_exp $key, $type, $table_schema, $values->{$key}->{$type}
                    => \@placeholder;
          }
        } # $type
      } else {
        push @and, _where_exp $key, '-eq', $table_schema, $values->{$key}
            => \@placeholder;
      } # $values->{$key}
    } # $values

    if (@and) {
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
