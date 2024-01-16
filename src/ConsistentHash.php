<?php

namespace Consistent\Hash;

class ConsistentHash
{
    private $nodes = [];
    private $positionToNode = [];
    private $replicationFactor = 64; // 虚拟节点数

    public function __construct()
    {
        $this->initNode();
    }

    public function initNode()
    {
        for ($i = 1; $i <= 100; $i++) {
            $this->addNode('node:' . $i); // 添加节点
        }
    }

    //存储节点
    public function addNode($node) {
        for ($i = 0; $i < $this->replicationFactor; $i++) {
            $virtualNode = "{$node}-{$i}";
            $hash = $this->hash($virtualNode);
            $this->nodes[$hash] = $virtualNode;
            $this->positionToNode[$hash] = $node;
        }
        ksort($this->nodes);
    }

    //删除节点
    public function removeNode($node) {
        for ($i = 0; $i < $this->replicationFactor; $i++) {
            $virtualNode = "{$node}-{$i}";
            $hash = $this->hash($virtualNode);
            unset($this->nodes[$hash]);
            unset($this->positionToNode[$hash]);
        }
    }

    //获取节点
    public function getNode($subject) {
        $hash = $this->hash($subject);

        foreach ($this->nodes as $nodeHash => $node) {
            if ($hash <= $nodeHash) {
                return $this->positionToNode[$nodeHash];
            }
        }

        return $this->positionToNode[reset($this->nodes)];
    }

    private function hash($string) {
        return sprintf('%u', crc32($string));
    }
}