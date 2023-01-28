<?php

declare(strict_types=1);

namespace EasyBuilder;

use PDO;
use PDOException;
use PDOStatement;
use Closure;
use Exception;
use InvalidArgumentException;

class Expression
{
    /**
     * Create a new raw query expression.
     *
     * @param  mixed  $value
     * @return void
     */
    public function __construct(protected mixed $value)
    {
        $this->value = $value;
    }

    /**
     * Get the value of the expression.
     *
     * @return mixed
     */
    public function getValue() : mixed
    {
        return $this->value;
    }
}

class QueryBuilder
{
    /**
     * The database connection instance.
     *
     * @var \PDO
     */
    protected static $pdo;

    /**
     * The columns that should be returned.
     *
     * @var string
     */
    protected $select = '';

    /**
     * Indicates if the query returns distinct results.
     *
     * @var string
     */
    protected $distinct = '';

    /**
     * The table which the query is targeting.
     *
     * @var string
     */
    protected $from = '';

    /**
     * The table joins for the query.
     *
     * @var string
     */
    protected $join = '';

    /**
     * The where constraints for the query.
     *
     * @var string
     */
    protected $where = '';

    /**
     * The groupings for the query.
     *
     * @var string
     */
    protected $group_by = '';

    /**
     * The having constraints for the query.
     *
     * @var string
     */
    protected $having = '';

    /**
     * The orderings for the query.
     *
     * @var string
     */
    protected $order_by = '';

    /**
     * The maximum number of records to return.
     *
     * @var string
     */
    protected $limit = '';

    /**
     * The number of records to skip.
     *
     * @var string
     */
    protected $offset = '';

    /**
     * The query union statements.
     *
     * @var string
     */
    protected $union = '';

    /**
     * The maximum number of union records to return.
     *
     * @var string
     */
    protected $unionLimit = '';

    /**
     * The number of union records to skip.
     *
     * @var string
     */
    protected $unionOffset = '';

    /**
     * The orderings for the union query.
     *
     * @var string
     */
    protected $unionOrder = '';

    /**
     * The current query value bindings.
     *
     * @var array
     */
    protected $bindings = [
        'select' => [],
        'from' => [],
        'join' => [],
        'where' => [],
        'groupBy' => [],
        'having' => [],
        'order' => [],
        'union' => [],
        'unionOrder' => []
    ];

    /**
     * All of the available clause operators.
     *
     * @var array
     */
    protected $operators = [
        '=', '<', '>', '<=', '>=', '<>', '!=', '<=>',
        'between', 'not between',
        'in', 'not in',
        'is', 'is not',
        'like', 'not like'
    ];

    /**
     * The PDO fetch parameters to use
     *
     * @var array
     */
    protected $fetchModeParams;

    /**
     * Class constructor.
     *
     * @return void
     */
    public function __construct()
    {
        
    }

    /**
     * Connect to the database.
     *
     * @param  array|\PDO  $config
     * @return \PDO
     * 
     * @throws \InvalidArgumentException
     * @throws \PDOException
     */
    public static function connect(array|PDO $config) : PDO
    {
        if ($config instanceof PDO) {
            static::$pdo = $config;
        } else if (!(static::$pdo instanceof PDO)) {
            try {
                $driver      = $config['driver']      ?? null;
                $host        = $config['host']        ?? null;
                $port        = $config['port']        ?? null;
                $dbname      = $config['database']    ?? null;
                $username    = $config['username']    ?? null;
                $password    = $config['password']    ?? null;
                $unix_socket = $config['unix_socket'] ?? null;
                $charset     = $config['charset']     ?? null;
                $collation   = $config['collation']   ?? null;
                $prefix      = $config['prefix']      ?? '';
                $options     = [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_ORACLE_NULLS => PDO::NULL_EMPTY_STRING,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_OBJ
                ];

                // merge developer supplied options with default
                foreach ($config['options'] ?? [] as $key => $value) {
                    $options[$key] = $value;
                }

                // validate driver
                if (!in_array($driver, ['mysql', 'sqlite', 'pgsql'])) {
                    throw new InvalidArgumentException("Unsupported PDO driver: {$driver}.");
                }

                // generate dsn
                $dsn = array_filter(match (true) {
                    $driver === 'sqlite' => compact('dbname'),
                    $driver === 'mysql' && $unix_socket => compact('unix_socket', 'dbname', 'charset'),
                    default => compact('host', 'port', 'dbname', 'charset')
                });

                $dsn = $driver.':'.implode(';', array_map(fn($key, $val) => "$key=$val", array_keys($dsn), $dsn));

                // make connection
                static::$pdo = new PDO($dsn, $username, $password, $options);
            } catch (PDOException $e) {
                throw new PDOException($e->getMessage());
            }
        }

        return static::$pdo;
    }

    /**
     * Get the current PDO connection.
     *
     * @return \PDO
     * @throws \PDOException
     */
    public static function getPdo() : PDO
    {
        if (!(static::$pdo instanceof PDO)) {
            throw new PDOException('No database connection found.');
        }

        return static::$pdo;
    }

    /**
     * Initialize the class and
     * Set the database table associated with the query.
     *
     * @return \EasyBuilder\QueryBuilder
     */
    public static function query()
    {
        return new static;
    }

    /**
     * Initialize the class and
     * Set the database table associated with the query.
     *
     * @param  string  $table
     * @return \EasyBuilder\QueryBuilder
     */
    public static function table($table, $as=null)
    {
        return static::query()->from($table, $as);
    }

    /**
     * Create a raw database expression.
     *
     * @param  mixed  $value
     * @return Expression
     */
    public static function raw(mixed $value) : Expression
    {
        return new Expression($value);
    }

    /**
     * Set the columns to be selected.
     *
     * @param  array|mixed  $columns
     * @return $this
     */
    public function select($columns = ['*'])
    {
        $this->select = '';
        $this->bindings['select'] = [];
        return $this->addSelect(is_array($columns) ? $columns : func_get_args());
    }

    /**
     * Add a new select column to the query.
     *
     * @param  array|mixed  $column
     * @return $this
     */
    public function addSelect($column)
    {
        $columns = is_array($column) ? $column : func_get_args();

        foreach ($columns as $as => $column) {
            if (is_string($as) && $column instanceof Closure) {
                $column($query = new static);
                $this->addBinding($query->getBindings(), 'select')->addSelect($this->raw('('.$query->toSql().')'), $as);
            } else {
                $this->select .= (empty($this->select) ? "" : ", ").$this->wrap($column);
            }
        }

        return $this;
    }

    /**
     * Force the query to only return distinct results.
     *
     * @return $this
     */
    public function distinct()
    {
        $this->distinct = 'DISTINCT';
        return $this;
    }

    /**
     * Set the table which the query is targeting.
     *
     * @param  \Closure|string  $table
     * @param  string  $as
     * @return $this
     * 
     * @throws \InvalidArgumentException
     */
    public function from(Closure|string $table, ?string $as=null)
    {
        if ($table instanceof Closure) {
            if (is_null($as)) {
                throw new InvalidArgumentException('The second parameter is required.');
            }

            $table($query = new static);
            $this->addBinding($query->getBindings(), 'from')->from($this->raw('('.$query->toSql().')'), $as);
        } else {
            $this->from = $this->wrap($as ? "{$table} AS {$as}" : $table);
        }

        return $this;
    }

    /**
     * Add a join clause to the query.
     *
     * @param  string  $table
     * @param  string  $first
     * @param  string  $operator
     * @param  string  $second
     * @param  string  $type
     * @return $this
     */
    public function join($table, $first, $operator, $second, $type='INNER')
    {
        $this->join .= strtoupper($type)." JOIN {$this->wrap($table)} ";
        $this->join .= "ON {$this->wrap($first)} $operator {$this->wrap($second)} ";
        return $this;
    }

    /**
     * Add a left join to the query.
     *
     * @param  string  $table
     * @param  string  $first
     * @param  string  $operator
     * @param  string  $second
     * @return $this
     */
    public function leftJoin($table, $first, $operator, $second)
    {
        return $this->join($table, $first, $operator, $second, 'LEFT');
    }

    /**
     * Add a right join to the query.
     *
     * @param  string  $table
     * @param  string  $first
     * @param  string  $operator
     * @param  string  $second
     * @return $this
     */
    public function rightJoin($table, $first, $operator, $second)
    {
        return $this->join($table, $first, $operator, $second, 'RIGHT');
    }

    /**
     * Add a basic where clause to the query.
     *
     * @param  \Closure|string|array|Expression  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @param  string  $logic
     * @param  string  $method
     * @return $this
     * 
     * @throws \InvalidArgumentException
     */
    public function where($column, mixed $operator=null, mixed $value=null, string $logic='AND', string $method='where')
    {
        // If the 'where|having' clause is added for the first time, we will change the logical operator to
        // 'WHERE|HAVING (NOT)? (EXISTS)?' instead of the supplied 'AND|OR (NOT)? (EXISTS)?'.
        // else if the where|having clause '$this->$method' does not end with a logical operator, append the supplied $logic
        if (empty($this->$method)) {
            $this->$method .= preg_replace('/^(AND|OR|XOR)/', strtoupper($method), strtoupper($logic), 1).' ';
        } else if (!preg_match('/\b(WHERE|HAVING|AND|OR|XOR)\s(NOT\s)?(EXISTS\s)?\(?$/', strtoupper($this->$method))) {
            $this->$method .= sprintf(' %s ', strtoupper($logic));
        }

        // If the column is an array, we will assume it is an array of key-value pairs
        // and can add them as where|having clause wrapped in parentheses. We will maintain the logical operator ($logic)
        // received when the method was called and pass it into each where|having clause.
        if (is_array($column)) {
            return $this->$method(
                function($query) use ($column, $logic, $method) {
                    foreach ($column as $key => $value) {
                        if (is_numeric($key) && is_array($value)) {
                            $query->$method(...array_slice(array_values($value), 0, 4));
                        } else {
                            $query->$method($key, '=', $value, $logic);
                        }
                    }
                }, logic: $logic
            );
        }

        // Here we will make some assumptions about the operator. If only 2 values are
        // passed to the method, we will assume that the operator is an equals sign.
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        // If the column is actually a \Closure instance, we will assume the developer
        // wants to begin a nested where|having statement which is wrapped in parentheses.
        // We will execute the \Closure passing in $this as a parameter.
        if ($column instanceof Closure && is_null($operator)) {
            $this->$method .= '(';
            $column($this);
            $this->$method .= ')';

            return $this;
        }

        // If the column is a \Closure instance and there is an operator value, we will
        // assume the developer wants to run a subquery and then compare the result
        // of that subquery with the given value that was provided to the method.
        if ($column instanceof Closure && !is_null($operator)) {
            $column($query = new static);
            return $this->addBinding($query->getBindings(), 'where')
                        ->$method($this->raw('('.$query->toSql().')'), $operator, $value, $logic);
        }

        // If the value is a \Closure, it means the developer is performing an entire
        // sub-select within the query and we will need to compile the sub-select
        // within the where|having clause to get the appropriate query record results.
        if ($value instanceof Closure) {
            $value($query = new static);
            return $this->addBinding($query->getBindings(), 'where')
                        ->$method($column, $operator, $this->raw('('.$query->toSql().')'), $logic);
        }

        // validate the operator
        if (!in_array($operator, $this->operators) && ($operator !== '__BLANK__')) {
            throw new InvalidArgumentException('Illegal operator and value combination.');
        }

        // Add a where|having (exist)? clause to the query if the above conditions are not met
        $column    = $this->wrap($column);
        $operator  = $operator === '__BLANK__' ? '' : strtoupper($operator);
        $parameter = $this->parameter($value);

        // If the operator is 'BETWEEN' or 'NOT BETWEEN',
        // we will add a 'BETWEEN' or 'NOT BETWEEN' statement to the query.
        if (in_array($operator, ['BETWEEN', 'NOT BETWEEN'])) {
            if (!is_array($value) || count($value) != 2) {
                throw new InvalidArgumentException('Illegal operator and value combination.');
            }

            $parameter = implode(' AND ', $parameter);
        }

        // If the operator is 'IN' or 'NOT IN',
        // we will add an 'IN' or 'NOT IN' statement to the query.
        else if (in_array($operator, ['IN', 'NOT IN'])) {
            if (!is_array($value) || empty($value)) {
                throw new InvalidArgumentException('Illegal operator and value combination.');
            }

            $parameter = '('.implode(',', $parameter).')';
        }

        // If the value is "null", we will just assume the developer wants to add a
        // where null clause to the query. So, we will allow a short-cut here to
        // that method for convenience so the developer doesn't have to check.
        else if (is_null($value)) {
            if (!in_array($operator, ['=', 'IS', '<>', '!=', 'IS NOT'])) {
                throw new InvalidArgumentException('Illegal operator and value combination.');
            }

            $value     = $this->raw('NULL');
            $operator  = in_array($operator, ['=', 'IS']) ? 'IS' : 'IS NOT';
            $parameter = 'NULL';
        }

        // add query and bindings
        $this->$method .= "$column $operator $parameter";
        return $this->addBinding($value, 'where');
    }

    /**
     * Add an "or where" clause to the query.
     *
     * @param  \Closure|string|array|Expression  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @return $this
     */
    public function orWhere($column, mixed $operator=null, mixed $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($column, $operator, $value, 'OR');
    }

    /**
     * Add a "where not" clause to the query.
     *
     * @param  \Closure|string|array|Expression  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @return $this
     */
    public function whereNot($column, mixed $operator=null, mixed $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($column, $operator, $value, 'AND NOT');
    }

    /**
     * Add an "or where not" clause to the query.
     *
     * @param  \Closure|string|array|Expression  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @return $this
     */
    public function orWhereNot($column, mixed $operator=null, mixed $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($column, $operator, $value, 'OR NOT');
    }

    /**
     * Add a "where" clause comparing two columns to the query.
     *
     * @param  string|array  $first
     * @param  ?string  $operator
     * @param  ?string  $second
     * @return $this
     */
    public function whereColumn(string|array $first, ?string $operator=null, ?string $second=null)
    {
        [$second, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$second, $operator];

        return $this->where($first, $operator, $this->raw($this->wrap($second)), 'AND');
    }

    /**
     * Add an "or where" clause comparing two columns to the query.
     *
     * @param  string|array  $first
     * @param  ?string  $operator
     * @param  ?string  $second
     * @return $this
     */
    public function orWhereColumn(string|array $first, ?string $operator=null, ?string $second=null)
    {
        [$second, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$second, $operator];

        return $this->where($first, $operator, $this->raw($this->wrap($second)), 'OR');
    }

    /**
     * Add a "where in" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function whereIn(string $column, array $values)
    {
        return $this->where($column, 'IN', $values, 'AND');
    }

    /**
     * Add an "or where in" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orWhereIn(string $column, array $values)
    {
        return $this->where($column, 'IN', $values, 'OR');
    }

    /**
     * Add a "where not in" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function whereNotIn(string $column, array $values)
    {
        return $this->where($column, 'NOT IN', $values, 'AND');
    }

    /**
     * Add an "or where not in" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orWhereNotIn(string $column, array $values)
    {
        return $this->where($column, 'NOT IN', $values, 'OR');
    }

    /**
     * Add a "where null" clause to the query.
     *
     * @param  string  $column
     * @return $this
     */
    public function whereNull(string $column)
    {
        return $this->where($column, 'IS', $this->raw('NULL'), 'AND');
    }

    /**
     * Add an "or where null" clause to the query.
     *
     * @param  string  $column
     * @return $this
     */
    public function orWhereNull(string $column)
    {
        return $this->where($column, 'IS', $this->raw('NULL'), 'OR');
    }

    /**
     * Add a "where not null" clause to the query.
     *
     * @param  string  $column
     * @return $this
     */
    public function whereNotNull(string $column)
    {
        return $this->where($column, 'IS NOT', $this->raw('NULL'), 'AND');
    }

    /**
     * Add an "or where not null" clause to the query.
     *
     * @param  string  $column
     * @return $this
     */
    public function orWhereNotNull(string $column)
    {
        return $this->where($column, 'IS NOT', $this->raw('NULL'), 'OR');
    }

    /**
     * Add a "where between" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function whereBetween(string $column, array $values)
    {
        return $this->where($column, 'BETWEEN', $values, 'AND');
    }

    /**
     * Add an "or where between" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orWhereBetween(string $column, array $values)
    {
        return $this->where($column, 'BETWEEN', $values, 'OR');
    }

    /**
     * Add a "where not between" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function whereNotBetween(string $column, array $values)
    {
        return $this->where($column, 'NOT BETWEEN', $values, 'AND');
    }

    /**
     * Add an "or where not between" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orWhereNotBetween(string $column, array $values)
    {
        return $this->where($column, 'NOT BETWEEN', $values, 'OR');
    }

    /**
     * Add a where between statement using columns to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function whereBetweenColumns(string $column, array $values)
    {
        $values = array_map([$this, 'raw'], $this->wrap($values));
        return $this->where($column, 'BETWEEN', $values, 'AND');
    }

    /**
     * Add an or where between statement using columns to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orWhereBetweenColumns(string $column, array $values)
    {
        $values = array_map([$this, 'raw'], $this->wrap($values));
        return $this->where($column, 'BETWEEN', $values, 'OR');
    }

    /**
     * Add a where not between statement using columns to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function whereNotBetweenColumns(string $column, array $values)
    {
        $values = array_map([$this, 'raw'], $this->wrap($values));
        return $this->where($column, 'NOT BETWEEN', $values, 'AND');
    }

    /**
     * Add an or where not between statement using columns to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orWhereNotBetweenColumns(string $column, array $values)
    {
        $values = array_map([$this, 'raw'], $this->wrap($values));
        return $this->where($column, 'NOT BETWEEN', $values, 'OR');
    }

    /**
     * Add a "where date" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  ?string  $value
     * @return $this
     */
    public function whereDate(string $column, string $operator, ?string $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($this->raw("DATE({$this->wrap($column)})"), $operator, $value, 'AND');
    }

    /**
     * Add an "or where date" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  ?string  $value
     * @return $this
     */
    public function orWhereDate(string $column, string $operator, ?string $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($this->raw("DATE({$this->wrap($column)})"), $operator, $value, 'OR');
    }

    /**
     * Add a "where time" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  ?string  $value
     * @return $this
     */
    public function whereTime(string $column, string $operator, ?string $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($this->raw("TIME({$this->wrap($column)})"), $operator, $value, 'AND');
    }

    /**
     * Add an "or where time" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  ?string  $value
     * @return $this
     */
    public function orWhereTime(string $column, string $operator, ?string $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($this->raw("TIME({$this->wrap($column)})"), $operator, $value, 'OR');
    }

    /**
     * Add a "where day" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  int|string|null  $value
     * @return $this
     */
    public function whereDay(string $column, string $operator, int|string|null $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($this->raw("DAY({$this->wrap($column)})"), $operator, $value, 'AND');
    }

    /**
     * Add an "or where day" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  int|string|null  $value
     * @return $this
     */
    public function orWhereDay(string $column, string $operator, int|string|null $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($this->raw("DAY({$this->wrap($column)})"), $operator, $value, 'OR');
    }

    /**
     * Add a "where month" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  int|string|null  $value
     * @return $this
     */
    public function whereMonth(string $column, string $operator, int|string|null $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($this->raw("MONTH({$this->wrap($column)})"), $operator, $value, 'AND');
    }

    /**
     * Add an "or where month" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  int|string|null  $value
     * @return $this
     */
    public function orWhereMonth(string $column, string $operator, int|string|null $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($this->raw("MONTH({$this->wrap($column)})"), $operator, $value, 'OR');
    }

    /**
     * Add a "where year" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  int|string|null  $value
     * @return $this
     */
    public function whereYear(string $column, string $operator, int|string|null $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($this->raw("YEAR({$this->wrap($column)})"), $operator, $value, 'AND');
    }

    /**
     * Add an "or where year" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  int|string|null  $value
     * @return $this
     */
    public function orWhereYear(string $column, string $operator, int|string|null $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->where($this->raw("YEAR({$this->wrap($column)})"), $operator, $value, 'OR');
    }

    /**
     * Add an exists clause to the query.
     *
     * @param  \Closure  $callback
     * @return $this
     */
    public function whereExists(Closure $callback)
    {
        return $this->where($callback, '__BLANK__', $this->raw(''), 'AND EXISTS');
    }

    /**
     * Add an or exists clause to the query.
     *
     * @param  \Closure  $callback
     * @return $this
     */
    public function orWhereExists(Closure $callback)
    {
        return $this->where($callback, '__BLANK__', $this->raw(''), 'OR EXISTS');
    }

    /**
     * Add a where not exists clause to the query.
     *
     * @param  \Closure  $callback
     * @return $this
     */
    public function whereNotExists(Closure $callback)
    {
        return $this->where($callback, '__BLANK__', $this->raw(''), 'AND NOT EXISTS');
    }

    /**
     * Add a where not exists clause to the query.
     *
     * @param  \Closure  $callback
     * @return $this
     */
    public function orWhereNotExists(Closure $callback)
    {
        return $this->where($callback, '__BLANK__', $this->raw(''), 'OR NOT EXISTS');
    }

    /**
     * Add a "group by" clause to the query.
     *
     * @param  array|string  $columns
     * @return $this
     */
    public function groupBy(array|string $columns)
    {
        $columns = is_array($columns) ? $columns : func_get_args();
        $this->group_by = "GROUP BY ".implode(',', $this->wrap($columns));
        return $this;
    }

    /**
     * Add a basic having clause to the query.
     *
     * @param  \Closure|string|array  $column
     * @param  ?string  $operator
     * @param  mixed  $value
     * @param  string  $logic
     * @return $this
     */
    public function having($column, array|string $operator=null, array|string $value=null, string $logic='AND')
    {
        return $this->where($column, $operator, $value, $logic, 'having');
    }

    /**
     * Add an "or having" clause to the query.
     *
     * @param  \Closure|string|array|Expression  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @return $this
     */
    public function orHaving($column, mixed $operator=null, mixed $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($column, $operator, $value, 'OR');
    }

    /**
     * Add a "having not" clause to the query.
     *
     * @param  \Closure|string|array|Expression  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @return $this
     */
    public function havingNot($column, mixed $operator=null, mixed $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($column, $operator, $value, 'AND NOT');
    }

    /**
     * Add an "or having not" clause to the query.
     *
     * @param  \Closure|string|array|Expression  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @return $this
     */
    public function orHavingNot($column, mixed $operator=null, mixed $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($column, $operator, $value, 'OR NOT');
    }

    /**
     * Add a "having" clause comparing two columns to the query.
     *
     * @param  string|array  $first
     * @param  ?string  $operator
     * @param  ?string  $second
     * @return $this
     */
    public function havingColumn(string|array $first, ?string $operator=null, ?string $second=null)
    {
        [$second, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$second, $operator];

        return $this->having($first, $operator, $this->raw($this->wrap($second)), 'AND');
    }

    /**
     * Add an "or having" clause comparing two columns to the query.
     *
     * @param  string|array  $first
     * @param  ?string  $operator
     * @param  ?string  $second
     * @return $this
     */
    public function orHavingColumn(string|array $first, ?string $operator=null, ?string $second=null)
    {
        [$second, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$second, $operator];

        return $this->having($first, $operator, $this->raw($this->wrap($second)), 'OR');
    }

    /**
     * Add a "having in" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function havingIn(string $column, array $values)
    {
        return $this->having($column, 'IN', $values, 'AND');
    }

    /**
     * Add an "or having in" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orHavingIn(string $column, array $values)
    {
        return $this->having($column, 'IN', $values, 'OR');
    }

    /**
     * Add a "having not in" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function havingNotIn(string $column, array $values)
    {
        return $this->having($column, 'NOT IN', $values, 'AND');
    }

    /**
     * Add an "or having not in" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orHavingNotIn(string $column, array $values)
    {
        return $this->having($column, 'NOT IN', $values, 'OR');
    }

    /**
     * Add a "having null" clause to the query.
     *
     * @param  string  $column
     * @return $this
     */
    public function havingNull(string $column)
    {
        return $this->having($column, 'IS', $this->raw('NULL'), 'AND');
    }

    /**
     * Add an "or having null" clause to the query.
     *
     * @param  string  $column
     * @return $this
     */
    public function orHavingNull(string $column)
    {
        return $this->having($column, 'IS', $this->raw('NULL'), 'OR');
    }

    /**
     * Add a "having not null" clause to the query.
     *
     * @param  string  $column
     * @return $this
     */
    public function havingNotNull(string $column)
    {
        return $this->having($column, 'IS NOT', $this->raw('NULL'), 'AND');
    }

    /**
     * Add an "or having not null" clause to the query.
     *
     * @param  string  $column
     * @return $this
     */
    public function orHavingNotNull(string $column)
    {
        return $this->having($column, 'IS NOT', $this->raw('NULL'), 'OR');
    }

    /**
     * Add a "having between" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function havingBetween(string $column, array $values)
    {
        return $this->having($column, 'BETWEEN', $values, 'AND');
    }

    /**
     * Add an "or having between" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orHavingBetween(string $column, array $values)
    {
        return $this->having($column, 'BETWEEN', $values, 'OR');
    }

    /**
     * Add a "having not between" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function havingNotBetween(string $column, array $values)
    {
        return $this->having($column, 'NOT BETWEEN', $values, 'AND');
    }

    /**
     * Add an "or having not between" clause to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orHavingNotBetween(string $column, array $values)
    {
        return $this->having($column, 'NOT BETWEEN', $values, 'OR');
    }

    /**
     * Add a having between statement using columns to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function havingBetweenColumns(string $column, array $values)
    {
        $values = array_map([$this, 'raw'], $this->wrap($values));
        return $this->having($column, 'BETWEEN', $values, 'AND');
    }

    /**
     * Add an or having between statement using columns to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orHavingBetweenColumns(string $column, array $values)
    {
        $values = array_map([$this, 'raw'], $this->wrap($values));
        return $this->having($column, 'BETWEEN', $values, 'OR');
    }

    /**
     * Add a having not between statement using columns to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function havingNotBetweenColumns(string $column, array $values)
    {
        $values = array_map([$this, 'raw'], $this->wrap($values));
        return $this->having($column, 'NOT BETWEEN', $values, 'AND');
    }

    /**
     * Add an or having not between statement using columns to the query.
     *
     * @param  string  $column
     * @param  array  $values
     * @return $this
     */
    public function orHavingNotBetweenColumns(string $column, array $values)
    {
        $values = array_map([$this, 'raw'], $this->wrap($values));
        return $this->having($column, 'NOT BETWEEN', $values, 'OR');
    }

    /**
     * Add a "having date" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  ?string  $value
     * @return $this
     */
    public function havingDate(string $column, string $operator, ?string $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($this->raw("DATE({$this->wrap($column)})"), $operator, $value, 'AND');
    }

    /**
     * Add an "or having date" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  ?string  $value
     * @return $this
     */
    public function orHavingDate(string $column, string $operator, ?string $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($this->raw("DATE({$this->wrap($column)})"), $operator, $value, 'OR');
    }

    /**
     * Add a "having time" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  ?string  $value
     * @return $this
     */
    public function havingTime(string $column, string $operator, ?string $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($this->raw("TIME({$this->wrap($column)})"), $operator, $value, 'AND');
    }

    /**
     * Add an "or having time" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  ?string  $value
     * @return $this
     */
    public function orHavingTime(string $column, string $operator, ?string $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($this->raw("TIME({$this->wrap($column)})"), $operator, $value, 'OR');
    }

    /**
     * Add a "having day" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  int|string|null  $value
     * @return $this
     */
    public function havingDay(string $column, string $operator, int|string|null $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($this->raw("DAY({$this->wrap($column)})"), $operator, $value, 'AND');
    }

    /**
     * Add an "or having day" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  int|string|null  $value
     * @return $this
     */
    public function orHavingDay(string $column, string $operator, int|string|null $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($this->raw("DAY({$this->wrap($column)})"), $operator, $value, 'OR');
    }

    /**
     * Add a "having month" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  int|string|null  $value
     * @return $this
     */
    public function havingMonth(string $column, string $operator, int|string|null $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($this->raw("MONTH({$this->wrap($column)})"), $operator, $value, 'AND');
    }

    /**
     * Add an "or having month" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  int|string|null  $value
     * @return $this
     */
    public function orHavingMonth(string $column, string $operator, int|string|null $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($this->raw("MONTH({$this->wrap($column)})"), $operator, $value, 'OR');
    }

    /**
     * Add a "having year" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  int|string|null  $value
     * @return $this
     */
    public function havingYear(string $column, string $operator, int|string|null $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($this->raw("YEAR({$this->wrap($column)})"), $operator, $value, 'AND');
    }

    /**
     * Add an "or having year" statement to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  int|string|null  $value
     * @return $this
     */
    public function orHavingYear(string $column, string $operator, int|string|null $value=null)
    {
        [$value, $operator] = func_num_args() === 2 
            ? [$operator, '='] 
            : [$value, $operator];

        return $this->having($this->raw("YEAR({$this->wrap($column)})"), $operator, $value, 'OR');
    }

    /**
     * Add an exists clause to the query.
     *
     * @param  \Closure  $callback
     * @return $this
     */
    public function havingExists(Closure $callback)
    {
        return $this->having($callback, '__BLANK__', $this->raw(''), 'AND EXISTS');
    }

    /**
     * Add an or exists clause to the query.
     *
     * @param  \Closure  $callback
     * @return $this
     */
    public function orHavingExists(Closure $callback)
    {
        return $this->having($callback, '__BLANK__', $this->raw(''), 'OR EXISTS');
    }

    /**
     * Add a having not exists clause to the query.
     *
     * @param  \Closure  $callback
     * @return $this
     */
    public function havingNotExists(Closure $callback)
    {
        return $this->having($callback, '__BLANK__', $this->raw(''), 'AND NOT EXISTS');
    }

    /**
     * Add a having not exists clause to the query.
     *
     * @param  \Closure  $callback
     * @return $this
     */
    public function orHavingNotExists(Closure $callback)
    {
        return $this->having($callback, '__BLANK__', $this->raw(''), 'OR NOT EXISTS');
    }

    /**
     * Add an "order by" clause to the query.
     *
     * @param  string  $column
     * @param  string  $direction
     * @return $this
     * 
     * @throws \InvalidArgumentException
     */
    public function orderBy(string $column, string $direction='ASC')
    {
        $direction = strtoupper($direction);

        if (!in_array($direction, ['ASC', 'DESC']) && !(in_array($column, ['RAND()', 'RANDOM()']) && $direction === '')) {
            throw new InvalidArgumentException('Order direction must be "asc" or "desc".');
        }

        $this->order_by .= empty($this->order_by) ? "ORDER BY " : ", ";
        $this->order_by .= "{$this->wrap($column)} $direction";
        return $this;
    }

    /**
     * Add a descending "order by" clause to the query.
     *
     * @param  string  $column
     * @return $this
     */
    public function orderByDesc($column)
    {
        return $this->orderBy($column, 'DESC');
    }

    /**
     * Put the query's results in random order.
     *
     * @return $this
     */
    public function inRandomOrder()
    {
        $random = static::getPdo()->getAttribute(PDO::ATTR_DRIVER_NAME) === 'mysql' 
            ? 'RAND()' 
            : 'RANDOM()';

        return $this->orderBy($random, '');
    }

    /**
     * Set the "limit" value of the query.
     *
     * @param  int|string  $value
     * @return $this
     */
    public function limit(int|string $value)
    {
        $this->limit = "LIMIT $value";
        return $this;
    }

    /**
     * Set the "offset" value of the query.
     *
     * @param  int|string  $value
     * @return $this
     */
    public function offset(int|string $value)
    {
        $this->offset = "OFFSET $value";
        return $this;
    }

    /**
     * Get the SQL representation of the query.
     * Optionally replace parameter placeholder with the value of that parameter.
     *
     * @param  bool  $withValues
     * @return string
     */
    public function toSql(bool $withValues=false)
    {
        $select = $this->select ?: '*';
        $sql = trim(preg_replace("/\s\s+/", " ", 
           "SELECT {$this->distinct} {$select} 
            FROM {$this->from} 
            {$this->join} 
            {$this->where} 
            {$this->group_by} 
            {$this->having} 
            {$this->order_by} 
            {$this->limit} 
            {$this->offset}"
        ));

        if ($withValues) {
            $keys   = [];
            $values = $this->getBindings();

            # build a regular expression for each parameter
            foreach ($values as $key => $value) {
                $keys[] = is_string($key) ? "/:{$key}/" : "/[?]/";

                $values[$key] = match (true) {
                    is_string($value) => static::getPdo()->quote($value),
                    is_bool($value) => (int)$value,
                    default => $value
                };
            }

            $sql = preg_replace($keys, $values, $sql, 1);
        }

        return $sql;
    }

    /**
     * Set the fetch mode
     *
     * @param  mixed ...$params
     * @return $this
     */
    public function setFetchMode(mixed ...$params)
    {
        $this->fetchModeParams = $params;
        return $this;
    }

    /**
     * Execute the query as a "select" statement.
     * Return all selected columns and matched rows.
     * 
     * @param  array|string  $columns
     * @return array
     */
    public function get(array|string $columns=['*']) : array
    {
        // add $columns if no columns are selected already
        if (empty($this->select)) {
            $this->select(is_array($columns) ? $columns : func_get_args());
        }

        // execute query and return fetched data
        $stmt = $this->statement($this->toSql(), $this->getBindings());
        if (isset($this->fetchModeParams)) $stmt->setFetchMode(...$this->fetchModeParams);
        return $stmt->fetchAll();
    }

    /**
     * Execute the query as a "select" statement.
     * Return the first result of a query.
     *
     * @param  array|string  $columns
     * @return ?object
     */
    public function first(array|string $columns=['*']) : ?object
    {
        return $this->limit(1)->get(is_array($columns) ? $columns : func_get_args())[0] ?? null;
    }

    /**
     * Execute a query for a single record by ID.
     *
     * @param  int|string  $id
     * @param  array  $columns
     * @return ?object
     */
    public function find(int|string $id, $columns=['*']) : ?object
    {
        return $this->where('id', '=', $id)->first($columns);
    }

    /**
     * Get a single column's value from the first result of a query.
     *
     * @param  string  $column
     * @return mixed
     */
    public function value($column)
    {
        $result = (array) $this->first($column);
        return $result[$column] ?? null;
    }

    /**
     * Get the values of a given column.
     * The result may be keyed by the $key param.
     *
     * @param  string  $column
     * @param  ?string  $key
     * @return array
     */
    public function pluck(string $column, ?string $key=null) : array
    {
        $result = $this->get(is_null($key) ? [$column] : [$column, $key]);
        return array_column($result, $column, $key);
    }

    // /**
    //  * Execute the query as a "select" statement.
    //  * Return all selected columns and matched rows after applying callback.
    //  * 
    //  * @param  \Closure  $callback
    //  * @return array
    //  */
    // public function each(Closure $callback) : array
    // {
    //     // execute query
    //     $stmt = $this->statement($this->toSql(), $this->getBindings());

    //     // process fetched data
    //     $data = [];
    //     while ($row=$stmt->fetch()) {
    //         $callback($row);
    //         $data[] = $row;
    //     }

    //     return $data;
    // }

    /**
     * Execute an aggregate function on the database.
     *
     * @param  string  $function
     * @param  string  $column
     * @return mixed
     */
    public function aggregate(string $function, string $column='*')
    {
        $column = $this->raw(strtoupper($function)."({$this->wrap($column)}) AS `aggregate`");
        $result = (array) $this->select($column)->first();
        return $result['aggregate'] ?? null;
    }

    /**
     * Retrieve the "count" result of the query.
     *
     * @param  string  $column
     * @return int
     */
    public function count(string $column = '*')
    {
        return (int)$this->aggregate(__FUNCTION__, $column);
    }

    /**
     * Retrieve the minimum value of a given column.
     *
     * @param  string  $column
     * @return mixed
     */
    public function min(string $column)
    {
        return $this->aggregate(__FUNCTION__, $column);
    }

    /**
     * Retrieve the maximum value of a given column.
     *
     * @param  string  $column
     * @return mixed
     */
    public function max(string $column)
    {
        return $this->aggregate(__FUNCTION__, $column);
    }

    /**
     * Retrieve the sum of the values of a given column.
     *
     * @param  string  $column
     * @return mixed
     */
    public function sum(string $column)
    {
        return $this->aggregate(__FUNCTION__, $column) ?: 0;
    }

    /**
     * Retrieve the average of the values of a given column.
     *
     * @param  string  $column
     * @return mixed
     */
    public function avg(string $column)
    {
        return $this->aggregate(__FUNCTION__, $column);
    }

    /**
     * Determine if any rows exist for the current query.
     *
     * @return bool
     */
    public function exists()
    {
        return $this->count() > 0 ? true : false;
    }

    /**
     * Determine if no rows exist for the current query.
     *
     * @return bool
     */
    public function doesntExist()
    {
        return !$this->exists();
    }

    /**
     * Insert new records into the database.
     *
     * @param  array  $data
     * @return ?int
     */
    public function insert(array $data) : ?int
    {
        if (empty($data)) return null;

        $query      = static::query();
        $is_pgsql   = static::getPdo()->getAttribute(PDO::ATTR_DRIVER_NAME) === 'pgsql';
        $data       = array_is_list($data) ? $data : [$data];
        $columns    = implode(',', array_keys($data[0]));
        $parameters = [];

        // add values placeholder and bind values
        foreach ($data as $row) {
            $parameters[] = '('.implode(',', $this->parameter(array_values($row))).')';
            $query->addBinding(array_values($row));
        }

        // execute query
        $stmt = $this->statement(
            "INSERT INTO {$this->from} ({$columns}) VALUES".implode(',', $parameters).($is_pgsql ? " RETURNING id" : ""),
            $query->getBindings()
        );

        // return inserted id
        return (int)($is_pgsql ? $stmt->fetchColumn() : static::getPdo()->lastInsertId()) ?: null;
    }

    /**
     * Update records in the database.
     *
     * @param  array $data
     * @return ?int
     */
    public function update(array $data) : ?int
    {
        if (empty($data)) return null;

        // generate column = ? parameters and bindings
        $columns    = array_keys($data);
        $parameters = implode(',', array_map(fn($k,$v) => "$k = $v", $columns, $this->parameter(array_values($data))));
        $bindings   = static::query()->addBinding(array_values($data))->getBindings();

        // execute query and return affected rows
        $stmt = $this->statement(
            "UPDATE {$this->from} SET {$parameters} {$this->where}",
            array_merge($bindings, $this->bindings['where'])
        );
        return $stmt->rowCount();
    }

    /**
     * Increment a column's value by a given amount.
     *
     * @param  string  $column
     * @param  float|int  $amount
     * @param  array  $extra
     * @return int
     *
     * @throws \InvalidArgumentException
     */
    public function increment(string $column, float|int $amount=1, array $extra=[])
    {
        if (!is_numeric($amount)) {
            throw new InvalidArgumentException('Non-numeric value passed to increment method.');
        }

        $data = array_merge([$column => $this->raw("{$this->wrap($column)} + $amount")], $extra);
        return $this->update($data);
    }

    /**
     * Decrement a column's value by a given amount.
     *
     * @param  string  $column
     * @param  float|int  $amount
     * @param  array  $extra
     * @return int
     *
     * @throws \InvalidArgumentException
     */
    public function decrement(string $column, float|int $amount=1, array $extra=[])
    {
        if (!is_numeric($amount)) {
            throw new InvalidArgumentException('Non-numeric value passed to decrement method.');
        }

        $data = array_merge([$column => $this->raw("{$this->wrap($column)} - $amount")], $extra);
        return $this->update($data);
    }

    /**
     * Delete records from the database.
     *
     * @param  int|string|null  $id
     * @return int
     */
    public function delete(int|string $id=null) : int
    {
        if (!is_null($id)) {
            $this->where('id', '=', $id);
        }

        // execute query and return affected rows
        $stmt = $this->statement(
            "DELETE FROM {$this->from} {$this->where}",
            $this->bindings['where']
        );
        return $stmt->rowCount();
    }

    /**
     * Run a truncate statement on the table.
     *
     * @return void
     */
    public function truncate()
    {
        $this->statement("TRUNCATE {$this->from}");
    }

    /**
     * Apply the callback if the given "value" is (or resolves to) truthy.
     *
     * @param  mixed  $value
     * @param  \Closure  $callback
     * @param  ?\Closure  $default
     * @return $this
     */
    public function when(mixed $value, Closure $callback, ?Closure $default=null)
    {
        $value = $value instanceof Closure ? $value($this) : $value;

        if ($value) {
            $callback($this, $value);
        } else if ($default) {
            $default($this, $value);
        }

        return $this;
    }

    /**
     * Add a binding to the query.
     *
     * @param  mixed  $value
     * @param  string  $type
     * @return $this
     * 
     * @throws \InvalidArgumentException
     */
    public function addBinding(mixed $value, string $type='where')
    {
        if (!array_key_exists($type, $this->bindings)) {
            throw new InvalidArgumentException("Invalid binding type: {$type}.");
        }

        if ($value instanceof Expression) {
            // Ignore it's already part of the sql
        } else if (is_array($value)) {
            $this->bindings[$type] = array_merge($this->bindings[$type], $value);
        } else {
            $this->bindings[$type][] = $value;
        }

        return $this;
    }

    /**
     * Get the current query value bindings.
     *
     * @return array
     */
    public function getBindings()
    {
        return array_merge([], ...array_values($this->bindings));
    }
    
    /**
     * Start a transaction.
     *
     * @param \Closure  $callback
     * @return mixed
     * 
     * @throws \Exception
     */
    public static function transaction(Closure $callback) : mixed
    {
        static::getPdo()->beginTransaction();
        try {
            $result = $callback();
            static::getPdo()->commit();
        } catch (Exception $e) {
            static::getPdo()->rollBack();
            throw $e;
        }

        return $result;
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param  string  $sql
     * @param  array  $bindings
     * @return PDOStatement
     */
    public static function statement(string $sql, array $bindings=[]) : PDOStatement
    {
        // $start = microtime(true);
        $stmt = static::getPdo()->prepare($sql);
        foreach ($bindings as $key => $value) {
            $stmt->bindValue(
                is_string($key) ? $key : $key + 1,
                $value,
                match (true) {
                    is_int($value)      => PDO::PARAM_INT,
                    is_bool($value)     => PDO::PARAM_INT,
                    is_resource($value) => PDO::PARAM_LOB,
                    default             => PDO::PARAM_STR
                }
            );
        }
        $stmt->execute();
        // $time = microtime(true) - $start; // pass this up

        return $stmt;
    }

    /**
     * Wrap a value in keyword identifiers.
     *
     * @param  array|mixed  $value
     * @return array|mixed
     */
    public static function wrap(mixed $value) : mixed
    {
        if (is_array($value)) {
            return array_map(__METHOD__, $value);
        } else if ($value instanceof Expression) {
            return $value->getValue();
        } else if (stripos($value, ' as ') !== false) {
            return implode(' AS ', array_map(__METHOD__, preg_split('/\s+as\s+/i', $value)));
        } else {
            return implode('.', array_map(fn($v) => $v === '*' ? $v : "`$v`", explode('.', $value)));
        }
    }

    /**
     * Get the appropriate query parameter placeholder for a value(s).
     *
     * @param  array|mixed  $value
     * @return array|mixed
     */
    public static function parameter(mixed $value) : mixed
    {
        if (is_array($value)) {
            return array_map(__METHOD__, $value);
        } else {
            return $value instanceof Expression ? $value->getValue() : '?';
        }
    }
}