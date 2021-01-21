package tests

import (
	"github.com/tictf/client-validator/mocktikv"
	"github.com/tictf/client-validator/stub"
	"github.com/tictf/client-validator/validator"
)

type testTxnKV struct{}

func (t testTxnKV) newCluster(ctx validator.ExecContext) *mocktikv.Cluster {
	cluster, err := mocktikv.NewCluster(*mockTiKVAddr)
	ctx.AssertNil(err)
	return cluster
}

func (t testTxnKV) newClient(ctx validator.ExecContext) (*mocktikv.Cluster, *stub.TxnClientStub) {
	cluster := t.newCluster(ctx)
	client, err := stub.NewTxnClientStub(*clientProxyAddr, cluster.PDAddrs())
	ctx.AssertNil(err)
	return cluster, client
}

func (t testTxnKV) begin(ctx validator.ExecContext) (*mocktikv.Cluster, *stub.TxnClientStub,*stub.TransactionStub) {
	cluster,client:=t.newClient(ctx)
	txn,err := client.Begin()
	ctx.AssertNil(err)
	return cluster,client,txn
}

func (t testTxnKV) iter(ctx validator.ExecContext) (*mocktikv.Cluster, *stub.TxnClientStub,*stub.IteratorStub) {
	cluster,client,txn :=t.begin(ctx)
	iterator,_:= txn.Iter([]byte(""),[]byte(""))
	return cluster,client,iterator
}

var _ = validator.RegisterFeature("txnkv.new", "create a txnkv client", nil, testTxnKV{}.checkClientCreate)

func (t testTxnKV) checkClientCreate(ctx validator.ExecContext) validator.FeatureStatus {
	cluster := t.newCluster(ctx)
	defer cluster.Close()
	client, err := stub.NewTxnClientStub(*clientProxyAddr, cluster.PDAddrs())
	if err != nil {
		return errToFeatureStatus(err)
	}
	defer client.Close()
	return validator.FeaturePass
}

var _ = validator.RegisterFeature("txnkv.close", "create a txnkv client", nil, testTxnKV{}.checkClientClose)

func (t testTxnKV) checkClientClose(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client := t.newClient(ctx)
	defer cluster.Close()
	err := client.Close()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.begin", "create a txnkv client", nil, testTxnKV{}.checkClientBegin)

func (t testTxnKV) checkClientBegin(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client := t.newClient(ctx)
	defer cluster.Close()
	defer client.Close()
	_, err := client.Begin()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.begin-with-ts", "create a txnkv client", nil, testTxnKV{}.checkClientBeginWithTS)

func (t testTxnKV) checkClientBeginWithTS(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client := t.newClient(ctx)
	defer cluster.Close()
	defer client.Close()
	_, err := client.BeginWithTS(1611120033)
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.get-ts", "create a txnkv client", nil, testTxnKV{}.checkGetTS)

func (t testTxnKV) checkGetTS(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client := t.newClient(ctx)
	defer cluster.Close()
	defer client.Close()
	ts, err := client.GetTS()
	if err != nil {
		errToFeatureStatus(err)
	}
	ctx.AssertEQ(ts,1611120033,"expect 1611120033")
	return errToFeatureStatus(err)
}
var _ = validator.RegisterFeature("txnkv.set", "create a txnkv client", nil, testTxnKV{}.checkTxnSet)

func (t testTxnKV) checkTxnSet(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, txn:= t.begin(ctx)
	defer cluster.Close()
	defer client.Close()
	err :=txn.Set([]byte("k1"), []byte("v1"))
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.get", "create a txnkv client", nil, testTxnKV{}.checkTxnGet)

func (t testTxnKV) checkTxnGet(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, txn:= t.begin(ctx)
	defer cluster.Close()
	defer client.Close()
	val,err := txn.Get([]byte("k1"))
	if err != nil {
		return errToFeatureStatus(err)
	}
	ctx.AssertEQ(val,[]byte("v1"))
	return validator.FeaturePass
}

var _ = validator.RegisterFeature("txnkv.batch-get", "create a txnkv client", nil, testTxnKV{}.checkTxnBatchGet)

func (t testTxnKV) checkTxnBatchGet(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client := t.newClient(ctx)
	defer cluster.Close()
	err := client.Close()
	return errToFeatureStatus(err)
}



var _ = validator.RegisterFeature("txnkv.iter", "create a txnkv client", nil, testTxnKV{}.checkTxnIter)

func (t testTxnKV) checkTxnIter(ctx validator.ExecContext) validator.FeatureStatus {
	cluster,client,txn :=t.begin(ctx)
	defer cluster.Close()
	defer client.Close()
	_,err := txn.Iter([]byte(""),[]byte(""))
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.iter-reverse", "create a txnkv client", nil, testTxnKV{}.checkTxnIterReverse)

func (t testTxnKV) checkTxnIterReverse(ctx validator.ExecContext) validator.FeatureStatus {
	cluster,client,txn :=t.begin(ctx)
	defer cluster.Close()
	defer client.Close()
	_,err := txn.IterReverse([]byte(""))
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.readonly", "create a txnkv client", nil, testTxnKV{}.checkTxnIsReadOnly)

func (t testTxnKV) checkTxnIsReadOnly(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, txn:= t.begin(ctx)
	defer cluster.Close()
	defer client.Close()
	_,err :=txn.IsReadOnly()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.delete", "create a txnkv client", nil, testTxnKV{}.checkTxnDelete)

func (t testTxnKV) checkTxnDelete(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, txn:= t.begin(ctx)
	defer cluster.Close()
	defer client.Close()
	err := txn.Delete([]byte("k1"))
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.commit", "create a txnkv client", nil, testTxnKV{}.checkTxnCommit)

func (t testTxnKV) checkTxnCommit(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, txn:= t.begin(ctx)
	defer cluster.Close()
	defer client.Close()
	err := txn.Commit()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.rollback", "create a txnkv client", nil, testTxnKV{}.checkTxnRollback)

func (t testTxnKV) checkTxnRollback(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, txn:= t.begin(ctx)
	defer cluster.Close()
	defer client.Close()
	err := txn.Rollback()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.lock-keys", "create a txnkv client", nil, testTxnKV{}.checkTxnLockKeys)

func (t testTxnKV) checkTxnLockKeys(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, txn:= t.begin(ctx)
	defer cluster.Close()
	defer client.Close()
	err := txn.LockKeys()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.valid", "create a txnkv client", nil, testTxnKV{}.checkTxnValid)

func (t testTxnKV) checkTxnValid(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, txn:= t.begin(ctx)
	defer cluster.Close()
	defer client.Close()
	_,err := txn.Valid()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.len", "create a txnkv client", nil, testTxnKV{}.checkTxnLen)

func (t testTxnKV) checkTxnLen(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, txn:= t.begin(ctx)
	defer cluster.Close()
	defer client.Close()
	_,err := txn.Len()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.size", "create a txnkv client", nil, testTxnKV{}.checkTxnSize)

func (t testTxnKV) checkTxnSize(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, txn:= t.begin(ctx)
	defer cluster.Close()
	defer client.Close()
	_,err := txn.Size()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.iter-valid", "create a txnkv client", nil, testTxnKV{}.checkIterValid)

func (t testTxnKV) checkIterValid(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, iter:= t.iter(ctx)
	defer cluster.Close()
	defer client.Close()
	_,err := iter.Valid()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.iter-key", "create a txnkv client", nil, testTxnKV{}.ckeckIterKey)

func (t testTxnKV) ckeckIterKey(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, iter:= t.iter(ctx)
	defer cluster.Close()
	defer client.Close()
	_,err := iter.Key()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.iter-value", "create a txnkv client", nil, testTxnKV{}.ckeckIterValue)

func (t testTxnKV) ckeckIterValue(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, iter:= t.iter(ctx)
	defer cluster.Close()
	defer client.Close()
	_,err := iter.Value()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.iter-next", "create a txnkv client", nil, testTxnKV{}.checkIterNext)

func (t testTxnKV) checkIterNext(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, iter:= t.iter(ctx)
	defer cluster.Close()
	defer client.Close()
	err := iter.Next()
	return errToFeatureStatus(err)
}

var _ = validator.RegisterFeature("txnkv.iter-close", "create a txnkv client", nil, testTxnKV{}.checkIterClose)

func (t testTxnKV) checkIterClose(ctx validator.ExecContext) validator.FeatureStatus {
	cluster, client, iter:= t.iter(ctx)
	defer cluster.Close()
	defer client.Close()
	err := iter.Close()
	return errToFeatureStatus(err)
}