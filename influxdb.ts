import fetch, { Response } from 'node-fetch';

import http from 'http';
import https from 'https';
const httpAgent = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });
const agent = (_parsedURL: URL) => _parsedURL.protocol == 'https:' ? httpsAgent : httpAgent;

export class InfluxDb {
    apiToken: string;
    apiUrl: string;

    constructor(apiUrl: string, apiToken: string) {
        this.apiUrl = apiUrl;
        this.apiToken = apiToken;
    }

    get headers() {
        return {
            Authorization: 'Token ' + this.apiToken
        };
    }

    async createBucket(organizationId: string, name: string, shardDuration: number) {
        const bucket = {
            orgID: organizationId,
            name,
            shardGroupDuration: shardDuration
        };
        const url = this.apiUrl + '/buckets';
        const response = await fetch(url, {
            agent,
            method: 'POST',
            headers: this.headers,
            body: JSON.stringify(bucket),
        });
        await this.assertResponse(response);
        return await response.json();
    }

    async writeData(
        organizationName: string,
        bucketName: string,
        lines: string
    ) {
        const url = this.apiUrl + '/write?org=' + encodeURIComponent(organizationName) +
            '&bucket=' + bucketName + '&precision=ms';
        const response = await fetch(url, {
            agent,
            method: 'POST',
            headers: this.headers,
            body: lines
        });
        await this.assertResponse(response);
    }

    async deleteData(
        organizationName: string,
        bucketName: string,
        measurement: string,
        tags: {[key: string]: string}
    ) {
        const url = this.apiUrl + '/delete' +
            '?org=' + encodeURIComponent(organizationName) +
            '&bucket=' + encodeURIComponent(bucketName);
        let predicate = '_measurement="' + escapeMeasurement(measurement) + '"';
        for (const key in tags)
            predicate += ' AND ' + escapeTag(key) + '="' + escapeTag(key) + '"';
        const query = {
            predicate,
            start: '1970-01-02T00:00:00.000Z',
            stop: '2970-01-02T00:00:00.000Z'
        };
        const response = await fetch(url, {
            agent,
            headers: this.headers,
            method: 'POST',
            body: JSON.stringify(query)
        });
        await this.assertResponse(response);
    }

    private async assertResponse(response: Response) {
        if (!response.ok)
            throw new InfluxDbException(response.statusText + '\n' + await response.text());
    }
}

export class InfluxDbException extends Error {
    constructor(message: string) {
        super(message);
    }
}

function replaceText(text: string, oldSubString: string, newSubText: string): string {
    return text.split(oldSubString).join(newSubText);
}

function escapeMeasurement(measurement: string) {
    measurement = replaceText(measurement, ',', '\\,');
    measurement = replaceText(measurement, ' ', '\\ ');
    return measurement;
}

function escapeTag(tag: string) {
    tag = replaceText(tag, ',', '\\,');
    tag = replaceText(tag, '=', '\\=');
    tag = replaceText(tag, ' ', '\\ ');
    return tag;
}

export function buildPredicate(measurement: string, tags: {[key: string]: string}) {
    let predicate = '_measurement="' + escapeMeasurement(measurement) + '"';
    for (const key in tags)
        predicate += ' AND ' + escapeTag(key) + '="' + escapeTag(key) + '"';
    return predicate;
}
