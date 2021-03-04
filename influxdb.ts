import fetch, { Response } from 'node-fetch';

import http from 'http';
import https from 'https';
import papaparse from 'papaparse';
const httpAgent = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });
const agent = (_parsedURL: URL) => _parsedURL.protocol == 'https:' ? httpsAgent : httpAgent;

const MIN_DATE_STRING = '1970-01-02T00:00:00.000Z';
const MAX_DATE_STRING = '2970-01-02T00:00:00.000Z';

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

    async writeData(organizationName: string, bucketName: string, lines: string) {
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

    async readData(organizationName: string, query: string): Promise<string[][]> {
        const url = this.apiUrl + '/query?org=' + encodeURIComponent(organizationName);
        const requestObject = {
            query: query,
            type: 'flux'
        };
        const response = await fetch(url, {
            agent,
            method: 'POST',
            headers: this.headers,
            body: JSON.stringify(requestObject)
        });
        await this.assertResponse(response);
        const text = (await response.text()).trim();
        const table = papaparse.parse(text, { escapeChar: '\\' });
        if (table.errors != null && table.errors.length)
            throw new Error('Could not parse the response as CSV. Error: ' + table.errors[0]);
        return table.data as string[][];
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
            start: MIN_DATE_STRING,
            stop: MAX_DATE_STRING
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

export function buildTimedValueLine(
    measurement: string,
    tags: { [key: string] : string },
    value: number,
    timeStamp: number
) {
    let line = escapeMeasurement(measurement);
    for (const key in tags)
        line += ',' + escapeTag(key) + '=' + escapeTag(tags[key]);
    line += ' value=' + value + ' ' + timeStamp;
    return line;
}
