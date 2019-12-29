/*
 * SPDX-License-Identifier: Apache-2.0
 */

'use strict';

const { Contract } = require('fabric-contract-api');

class FabCar extends Contract {

    async initLedger(ctx) {
        console.info('============= START : Initialize Ledger ===========');
        const mts = [
            {
                tempA: 13,
                tempB: 12,
                tempC: 45,
                owner: 'Tomoko',
            },
            {
                tempA: 14,
                tempB: 14,
                tempC: 14,
                owner: 'Brad',
            },
            {
                tempA: 25,
                tempB: 25,
                tempC: 25,
                owner: 'Adriana',
            },
            {
                tempA: 25,
                tempB: 25,
                tempC: 26,
                owner: 'Michel',
            },
            {
                tempA: 27,
                tempB: 90,
                tempC: 0,
                owner: 'Aarav',
            },
            {
                tempA: 14,
                tempB: 14,
                tempC: 67,
                owner: 'Shotaro',
            },
        ];

        for (let j = 0; j < 40; j++) {
            await ctx.stub.deleteState('MT' + j);
        }

        for (let i = 0; i < mts.length; i++) {
            this.decide(mts[i]);
            mts[i].docType = 'measurement';
            await ctx.stub.putState('MT' + i, Buffer.from(JSON.stringify(mts[i])));
            console.info('Added <--> ', mts[i]);
        }
        console.info('============= END : Initialize Ledger ===========');
    }

    async decide(singleMt){
        if (singleMt.tempA == singleMt.tempB || singleMt.tempA == singleMt.tempC) {
            return singleMt.decision = singleMt.tempA;
        }
        if (singleMt.tempB == singleMt.tempC){
            return singleMt.decision = singleMt.tempB;
        }
        return singleMt.decision = "Rejected";
    }

    async createMt(ctx, mtNumber, tempA, tempB, tempC, owner) {
        console.info('============= START : Create Mt ===========');

        const car = {
            tempA,
            docType: 'measurement',
            tempB,
            tempC,
            owner,
        };

        this.decide(car);

        await ctx.stub.putState(mtNumber, Buffer.from(JSON.stringify(car)));
        console.info('============= END : Create Measurement ===========');
    }

    async queryAllMts(ctx) {
        const startKey = 'MT0';
        const endKey = 'MT999';

        const iterator = await ctx.stub.getStateByRange(startKey, endKey);

        const allResults = [];
        // eslint-disable-next-line no-constant-condition
        while (true) {
            const res = await iterator.next();

            if (res.value && res.value.value.toString()) {
                console.log(res.value.value.toString('utf8'));

                const Key = res.value.key;
                let Record;
                try {
                    Record = JSON.parse(res.value.value.toString('utf8'));
                } catch (err) {
                    console.log(err);
                    Record = res.value.value.toString('utf8');
                }
                allResults.push({ Key, Record });
            }
            if (res.done) {
                console.log('end of data');
                await iterator.close();
                console.info(allResults);
                return JSON.stringify(allResults);
            }
        }
    }

    async deleteMyAsset(ctx, myAssetId) {

        const exists = await this.myAssetExists(ctx, myAssetId);
        if (!exists) {
          throw new Error(`The my asset ${myAssetId} does not exist`);
        }
    
        await ctx.stub.deleteState(myAssetId);
    
      }

    async myAssetExists(ctx, myAssetId) {

        const buffer = await ctx.stub.getState(myAssetId);
        return (!!buffer && buffer.length > 0);
      }

}

module.exports = FabCar;
