<?php


namespace ramzzzes\Elaquent;


use Elasticsearch\ClientBuilder;
use GuzzleHttp\Client;

class Elaquent
{
    public $client;
    public $index;
    public $columnBetween;
    public $params;
    public $from;
    public $to;
    public $columnList;
    public $columnWhereNot;
    public $columnWhere;
    public $columnWhereIn;
    public $columnWhereInMultiple;
    public $sortColumn = '_id';
    public $sortDir = 'asc';
    public $groupByColumn;
    public $size = 10;
    public $offset = 1;

    public function __construct()
    {
        $this->client = ClientBuilder::create()->setHosts([
            'host' => env('ELASTIC_HOST')
        ]);
        $this->params = [];
        $this->params = [
            'size' => $this->size,
            'from' => $this->offset,
            'index' => $this->index,
        ];
    }

    public function index($index)
    {
        $this->params['index'] = $index;
        return $this;
    }

    public function select(array $columnList)
    {
        $this->columnList = $columnList;
        return $this;
    }

    public function orderBy($sortColumn,$sortDir)
    {
        $this->sortColumn = $sortColumn;
        $this->sortDir = $sortDir;
        return $this;
    }

    public function request($index,$method,$params = [])
    {
        try {
            $client = new Client();
            $response = $client->request('POST', env('ELASTIC_HOST') .'/'.$index.'/'.$method,[
                'headers' => [
                    'Content-Type' => 'application/json'
                ],
                'json' => $params
            ]);
            $res['code'] = 200;
            $res['data'] = json_decode($response->getBody()->getContents(), true);
        } catch (\Exception $e) {
            $res['code'] = $e->getCode();
            $res['data'] = urldecode(strip_tags($e->getMessage()));
        }

        return $res;
    }


    public function whereBetween($column,$from,$to,$size = 10)
    {
        $this->columnBetween = $column;
        $this->from = $from;
        $this->to = $to;
        $this->params['size'] = $size;


        if($this->columnBetween){
            $this->params['body']['query']['bool']['filter']['range'] = [
                $this->columnBetween => [
                    "gte" =>  $this->from,
                    "lte" =>  $this->to,
                ]
            ];
        }

        return $this;
    }

    public function whereNot($columnWhereNot,$value)
    {
        $this->columnWhereNot = $columnWhereNot;

        if($this->columnBetween){
            $this->params['body']['query']['bool']['must_not']['term'] = [
                $columnWhereNot.'.keyword' =>  $value
            ];
        }

        return $this;
    }

    public function whereNotNull($value)
    {
        if($this->columnBetween){
            $this->params['body']['query']['bool']['exists']['field'] = $value;
        }

        return $this;
    }

    public function where($columnWhere,$value)
    {
        $this->columnWhere = [
            $columnWhere => $value
        ];

        foreach ($this->columnWhere as $k=>$c) {
            $this->params['body']['query']['bool']['must'][]['match'] = [
                $k.'.keyword' =>  $value
            ];
        }

        return $this;
    }

    public function orWhere($columnWhere,$value)
    {
        $this->columnWhere = [
            $columnWhere => $value
        ];

        foreach ($this->columnWhere as $k=>$c) {
            $this->params['body']['query']['bool']['should'][]['match'] = [
                $k =>  $value
            ];
        }

        return $this;
    }

    public function whereLike($columnWhere,$value)
    {
        $this->columnWhere = $columnWhere;

        if($this->columnWhere){
            $this->params['body']['query']['bool']['must']['regexp'] = [
                $columnWhere   =>   '.*'.$value.'*.'
            ];
        }

        return $this;
    }

    public function orWhereLike($columnWhere,$value)
    {
        $this->columnWhere = $columnWhere;

        if($this->columnWhere){
            $this->params['body']['query']['bool']['should'][]['regexp'] = [
                $columnWhere   =>   '.*'.$value.'*.'
            ];
        }

        return $this;
    }

    public function whereIn($columnWhereIn,$list)
    {

        $this->columnWhereIn = $columnWhereIn;

        if($this->columnWhereIn){
            $this->params['body']['query']['bool']['must'][]['terms'] = [
                $columnWhereIn =>  $list
            ];
        }

        return $this;
    }

    public function whereNotIn($columnWhereIn,$list)
    {
        $this->columnWhereIn = $columnWhereIn;

        if($this->columnWhereIn){
            $this->params['body']['query']['bool']['must_not']['terms'] = [
                $columnWhereIn =>  $list
            ];
        }

        return $this;
    }

    public function whereInMultiple($columnWhereIn,$list)
    {
        $this->columnWhereInMultiple[] = $columnWhereIn;

        if($this->columnWhereInMultiple){
            foreach ($this->columnWhereInMultiple as $c) {
                $this->params['body']['query']['bool']['should']['terms'] = [
                    $c => $list
                ];
            }
        }

        return $this;
    }

    public function offset($offset)
    {
        $this->offset = $offset;

        return $this;
    }

    public function get($size = 10000)
    {


        if($this->columnList){
            $this->params['body']['_source'] = $this->columnList;
        }
//
        if($this->sortColumn){
            $this->params['body']['sort'][$this->sortColumn] = [
                'order' => $this->sortDir
            ];
        }


        if($size){
            $this->params['size'] = $size;
        }

        $this->params['from'] = $this->offset;

        $response = $this->client->build()->search($this->params);
        return $response['hits'];
    }

    public function groupBy($column,$desc = 'asc')
    {

        $this->groupByColumn = $column;

        $this->params['body']['aggs'][$this->groupByColumn] = [
            'terms' => [
                'field' => $this->groupByColumn.'.keyword',
                "order" =>  ["_key" => $desc ],
                "size" => 10000
            ]
        ];

        $response = $this->client->build()->search($this->params);


        return $response['aggregations'][$this->groupByColumn]['buckets'];

    }


}