const express = require('express');
const { Nebula } = require('@nebula-contrib/nebula-nodejs');
const app = express();
const port = 3000;

// Configure Nebula connection
const nebula = new Nebula({
  address: 'your_nebula_server_ip',
  username: 'your_username',
  password: 'your_password',
});

// Define a route to retrieve transactions for a specific address
app.get('/transactions/:address', async (req, res) => {
  const address = req.params.address;

  try {
    const session = await nebula.getSession();
    const query = `MATCH (a:Address {address: '${address}'})-[:SENT]->(t:Transaction) RETURN t`;
    const result = await session.execute(query);

    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).send('Error retrieving transactions');
  }
});

// Define a route to find transactions in a specific block
app.get('/transactions/block/:height', async (req, res) => {
  const blockHeight = req.params.height;

  try {
    const session = await nebula.getSession();
    const query = `MATCH (b:Block {height: ${blockHeight}})-[:CONTAINS]->(t:Transaction) RETURN t`;
    const result = await session.execute(query);

    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).send('Error finding transactions in the block');
  }
});

// Define a route to aggregate transaction data based on a time range
app.get('/aggregate-transactions', async (req, res) => {
    const { startTimestamp, endTimestamp } = req.query;
  
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (a:Address)-[:SENT]->(t:Transaction)
        WHERE t.timestamp >= ${startTimestamp} AND t.timestamp <= ${endTimestamp}
        RETURN a.address, COUNT(t) AS transaction_count
        ORDER BY transaction_count DESC
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error aggregating transaction data');
    }
  });
  

  // Define a route to find transactions involving multiple addresses
app.get('/transactions-involving-multiple-addresses', async (req, res) => {
    const { address1, address2 } = req.query;
  
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (a1:Address)-[:SENT]->(t:Transaction)-[:RECEIVED]->(a2:Address)
        WHERE a1.address = '${address1}' AND a2.address = '${address2}'
        RETURN t
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding transactions involving multiple addresses');
    }
  });


  // Define a route to find the largest transactions
app.get('/largest-transactions', async (req, res) => {
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)
        RETURN t
        ORDER BY t.value DESC
        LIMIT 10
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding the largest transactions');
    }
  });
  

  // Define a route to find transactions with a specific input address
app.get('/transactions-by-input-address/:inputAddress', async (req, res) => {
    const inputAddress = req.params.inputAddress;
  
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (a:Address {address: '${inputAddress}'})-[:SENT]->(t:Transaction)
        RETURN t
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding transactions with the specific input address');
    }
  });
  

  // Define a route to find transactions within a timestamp range
app.get('/transactions-by-timestamp-range', async (req, res) => {
    const { startTimestamp, endTimestamp } = req.query;
  
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)
        WHERE t.timestamp >= ${startTimestamp} AND t.timestamp <= ${endTimestamp}
        RETURN t
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding transactions within the timestamp range');
    }
  });


  // Define a route to find the total value sent by an address
app.get('/total-value-sent/:senderAddress', async (req, res) => {
    const senderAddress = req.params.senderAddress;
  
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (a:Address {address: '${senderAddress}'})-[:SENT]->(t:Transaction)
        RETURN SUM(t.value) AS total_sent
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding the total value sent by the address');
    }
  });
  


  // Define a route to find transactions with multiple inputs
app.get('/transactions-with-multiple-inputs', async (req, res) => {
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)
        WHERE size((t)-[:INPUT]->()) > 1
        RETURN t
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding transactions with multiple inputs');
    }
  });
  
  
  
  // Define a route to find transactions with no outputs (dust transactions)
app.get('/transactions-with-no-outputs', async (req, res) => {
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)
        WHERE size((t)-[:OUTPUT]->()) = 0
        RETURN t
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding transactions with no outputs');
    }
  });
  

  // Define a route to find transactions with OP_RETURN data
app.get('/transactions-with-op-return', async (req, res) => {
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)
        WHERE EXISTS((t)-[:OUTPUT]->(:OP_RETURN))
        RETURN t
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding transactions with OP_RETURN data');
    }
  });
  

  // Define a route to find transactions with a high number of confirmations
app.get('/transactions-with-high-confirmations/:threshold', async (req, res) => {
    const threshold = parseInt(req.params.threshold);
  
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)
        WHERE t.confirmations >= ${threshold}
        RETURN t
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding transactions with a high number of confirmations');
    }
  });
  


  // Define a route to find transactions involving addresses from a list
app.get('/transactions-involving-addresses', async (req, res) => {
    const addresses = req.query.addresses.split(',');
  
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)-[:SENT|RECEIVED]->(a:Address)
        WHERE a.address IN [${addresses.map((address) => `'${address}'`).join(', ')}]
        RETURN t
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding transactions involving addresses from the list');
    }
  });


  // Define a route to find transactions related to a specific block height
app.get('/transactions-by-block-height/:blockHeight', async (req, res) => {
    const blockHeight = parseInt(req.params.blockHeight);
  
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)-[:IN_BLOCK]->(b:Block)
        WHERE b.height = ${blockHeight}
        RETURN t
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding transactions related to the specific block height');
    }
  });
  
  
  // Define a route to identify large value transfers
app.get('/large-value-transfers/:threshold', async (req, res) => {
    const threshold = parseFloat(req.params.threshold);
  
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)-[:OUTPUT]->(a:Address)
        WHERE t.value >= ${threshold}
        RETURN t, a
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error identifying large value transfers');
    }
  });


  // Define a route to find transactions with unspent outputs (UTXOs)
app.get('/transactions-with-utxos', async (req, res) => {
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)-[:OUTPUT]->(a:Address)
        WHERE NOT (t)-[:INPUT]->(:TransactionOutput)
        RETURN t, a
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding transactions with unspent outputs (UTXOs)');
    }
  });
  
  
// Define a route to calculate transaction volume over time
app.get('/transaction-volume-over-time', async (req, res) => {
    const { startTimestamp, endTimestamp } = req.query;
  
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)
        WHERE t.timestamp >= ${startTimestamp} AND t.timestamp <= ${endTimestamp}
        RETURN date(datetime({{epochmillis: t.timestamp}})) AS date, SUM(t.value) AS volume
        ORDER BY date
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error calculating transaction volume over time');
    }
  });


  // Define a route to find transactions with multiple outputs to different addresses
app.get('/transactions-with-multiple-outputs', async (req, res) => {
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)
        WHERE size((t)-[:OUTPUT]->(:Address)) > 1
        RETURN t
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error finding transactions with multiple outputs to different addresses');
    }
  });
  
  
  // Define a route to identify self-transactions
app.get('/self-transactions', async (req, res) => {
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (a1:Address)-[:SENT]->(t:Transaction)-[:RECEIVED]->(a2:Address)
        WHERE a1 = a2
        RETURN t
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error identifying self-transactions');
    }
  });



  // Define a route to calculate transaction fees
app.get('/transaction-fees', async (req, res) => {
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)-[:INPUT]->(i:TransactionOutput)
        MATCH (t)-[:OUTPUT]->(o:Address)
        RETURN t, SUM(i.value) - SUM(o.value) AS fee
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error calculating transaction fees');
    }
  });


  // Define a route to identify high-frequency traders
app.get('/high-frequency-traders/:threshold', async (req, res) => {
    const threshold = parseInt(req.params.threshold);
  
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (a:Address)-[:SENT|RECEIVED]->(t:Transaction)
        WITH a, COUNT(t) AS transaction_count
        WHERE transaction_count >= ${threshold}
        RETURN a, transaction_count
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error identifying high-frequency traders');
    }
  });


  // Define a route to calculate the average transaction value over time
app.get('/average-transaction-value-over-time', async (req, res) => {
    const { startTimestamp, endTimestamp } = req.query;
  
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)
        WHERE t.timestamp >= ${startTimestamp} AND t.timestamp <= ${endTimestamp}
        RETURN date(datetime({{epochmillis: t.timestamp}})) AS date, AVG(t.value) AS avg_transaction_value
        ORDER BY date
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error calculating the average transaction value over time');
    }
  });
  
  
  

// Define a route to identify transaction chains
app.get('/transaction-chains', async (req, res) => {
    try {
      const session = await nebula.getSession();
      const query = `
        MATCH (t:Transaction)
        WHERE size((t)-[:INPUT]->()) > 1 AND size((t)-[:OUTPUT]->()) > 1
        RETURN t
      `;
      const result = await session.execute(query);
  
      res.json(result);
    } catch (error) {
      console.error(error);
      res.status(500).send('Error identifying transaction chains');
    }
  });
  

  
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
