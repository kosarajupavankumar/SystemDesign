import crypto from 'crypto';

class AdvancedConsistentHashing {
    private ring: Map<number, string>;
    private sortedKeys: number[];
    private numberOfReplicas: number;
    private weights: Map<string, number>;
    private hashFunction: (key: string) => number;

    constructor(
        numberOfReplicas: number, 
        hashFunction: (key: string) => number = AdvancedConsistentHashing.defaultHash
    ) {
        this.ring = new Map<number, string>();
        this.sortedKeys = [];
        this.numberOfReplicas = numberOfReplicas;
        this.weights = new Map<string, number>();
        this.hashFunction = hashFunction;
    }

    // Default hash function (MD5)
    private static defaultHash(key: string): number {
        const hash = crypto.createHash('md5').update(key).digest('hex');
        return parseInt(hash.substring(0, 8), 16);
    }

    // Add a node to the ring with an optional weight
    addNode(node: string, weight: number = 1): void {
        this.weights.set(node, weight);
        for (let i = 0; i < this.numberOfReplicas * weight; i++) {
            const replicaKey = `${node}:${i}`;
            const hashValue = this.hashFunction(replicaKey);
            this.ring.set(hashValue, node);
            this.sortedKeys.push(hashValue);
        }
        this.sortedKeys.sort((a, b) => a - b);
    }

    // Remove a node from the ring
    removeNode(node: string): void {
        const weight = this.weights.get(node) || 1;
        for (let i = 0; i < this.numberOfReplicas * weight; i++) {
            const replicaKey = `${node}:${i}`;
            const hashValue = this.hashFunction(replicaKey);
            this.ring.delete(hashValue);
            this.sortedKeys = this.sortedKeys.filter(key => key !== hashValue);
        }
        this.weights.delete(node);
    }

    // Get the node responsible for a given key
    getNode(key: string): string | undefined {
        if (this.ring.size === 0) {
            return undefined;
        }

        const hashValue = this.hashFunction(key);
        let index = this.sortedKeys.findIndex(k => k >= hashValue);

        if (index === -1) {
            index = 0;
        }

        return this.ring.get(this.sortedKeys[index]);
    }

    // Get a list of N distinct nodes for redundancy/fault tolerance
    getNodes(key: string, count: number): string[] {
        const result: string[] = [];
        const ringSize = this.sortedKeys.length;

        if (ringSize === 0 || count <= 0) {
            return result;
        }

        const hashValue = this.hashFunction(key);
        let index = this.sortedKeys.findIndex(k => k >= hashValue);

        if (index === -1) {
            index = 0;
        }

        while (result.length < count && result.length < ringSize) {
            const node = this.ring.get(this.sortedKeys[index]);
            if (node && !result.includes(node)) {
                result.push(node);
            }
            index = (index + 1) % ringSize;
        }

        return result;
    }
}

// Custom SHA-256 hash function (optional)
function customHash(key: string): number {
    const hash = crypto.createHash('sha256').update(key).digest('hex');
    return parseInt(hash.substring(0, 8), 16);
}

// Example usage
const ch = new AdvancedConsistentHashing(3, customHash);
ch.addNode('NodeA', 2); // Add with weight 2
ch.addNode('NodeB', 1); // Add with weight 1
ch.addNode('NodeC', 3); // Add with weight 3

console.log(ch.getNode('myKey1')); // Outputs the node responsible for 'myKey1'
console.log(ch.getNodes('myKey2', 2)); // Outputs two nodes responsible for 'myKey2' for redundancy

// Remove a node
ch.removeNode('NodeB');

console.log(ch.getNode('myKey1')); // Outputs the node responsible for 'myKey1' after NodeB is removed
