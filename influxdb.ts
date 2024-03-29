import fetch, { Response } from 'node-fetch';

import http from 'http';
import https from 'https';
import papaparse from 'papaparse';
const httpAgent = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });
const agent = (_parsedURL: URL) => _parsedURL.protocol == 'https:' ? httpsAgent : httpAgent;

// https://docs.influxdata.com/influxdb/v1.8/troubleshooting/frequently-asked-questions/#what-are-the-minimum-and-maximum-timestamps-that-influxdb-can-store
export const INFLUX_MIN_DATE_STRING = '1677-09-21T00:12:43.145224194Z';
export const INFLUX_MAX_DATE_STRING = '2262-04-11T23:47:16.854775806Z';

export class InfluxDb {
    apiToken: string;
    url: string;

    get apiUrl() {
        return this.url + '/api/v2';
    }

    constructor(url: string, apiToken: string) {
        this.url = url;
        this.apiToken = apiToken;
    }

    get headers() {
        return {
            Authorization: 'Token ' + this.apiToken
        };
    }

    async getOrganizationByName(organizationName: string) {
        const url = this.apiUrl + '/orgs?org=' + encodeURIComponent(organizationName);
        const response = await fetch(url, {
            agent,
            method: 'GET',
            headers: this.headers,
        });
        await this.assertResponse(response);
        const data: OrganizationsResponse = await response.json();
        return data.orgs[0];
    }

    async getAllOrganizations(): Promise<Organization[]> {
        let url = this.apiUrl + '/orgs';
        let organizations: Organization[] = [];
        while (url != null) {
            const response = await fetch(url, {
                agent,
                method: 'GET',
                headers: this.headers,
            });
            await this.assertResponse(response);
            const data: OrganizationsResponse = await response.json();
            organizations.push(...data.orgs);
            url = data.links.next ? this.url + data.links.next : null;
        }
        return organizations;
    }

    async createOrganization(organization: Organization): Promise<Organization> {

        const url = this.apiUrl + '/orgs';
        const response = await fetch(url, {
            agent,
            method: 'POST',
            headers: this.headers,
            body: JSON.stringify(organization),
        });
        await this.assertResponse(response);
        const data: Organization = await response.json();
        return data;
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
        const table = papaparse.parse(text, { escapeChar: '\\', delimiter: ',' });
        if (table.errors != null && table.errors.length)
            throw new Error('Could not parse the response as CSV. Error: ' + table.errors[0].message);
        return table.data as string[][];
    }

    async deleteData(
        organizationName: string,
        bucketName: string,
        measurement: string,
        tags: {[key: string]: string},
        start: string = INFLUX_MIN_DATE_STRING,
        stop: string = INFLUX_MAX_DATE_STRING
    ) {
        const url = this.apiUrl + '/delete' +
            '?org=' + encodeURIComponent(organizationName) +
            '&bucket=' + encodeURIComponent(bucketName);
        const predicate = buildPredicate(measurement, tags);
        const query = { predicate, start, stop };
        const response = await fetch(url, {
            agent,
            headers: this.headers,
            method: 'POST',
            body: JSON.stringify(query)
        });
        await this.assertResponse(response);
    }

    async getBuckets(organizationName: string): Promise<Bucket[]> {
        let url = this.apiUrl + '/buckets';
        if (organizationName)
            url += '?org=' + encodeURIComponent(organizationName);

        const buckets: Bucket[] = [];
        while (url) {
            const response = await fetch(url, {
                agent,
                headers: this.headers,
                method: 'GET'
            });
            await this.assertResponse(response);
            const responseObject = await response.json();
            for (const bucket of responseObject.buckets)
                buckets.push(bucket);
            url = responseObject.links.next;
        }
        return buckets;
    }

    /** @returns true if deleted */
    async deleteBucketByName(organizationName: string, bucketName: string): Promise<boolean> {
        const buckets = await this.getBuckets(organizationName);
        const bucket = buckets.find(b => b.name == bucketName);
        const found = bucket != null;
        if (found)
            await this.deleteBucket(bucket.id);
        return found;
    }

    async deleteBucket(bucketId: string) {
        const url = this.apiUrl + '/buckets/' + encodeURIComponent(bucketId);
        const response = await fetch(url, {
            agent,
            headers: this.headers,
            method: 'DELETE'
        });
        await this.assertResponse(response);
    }

    private async assertResponse(response: Response) {
        if (!response.ok)
            throw new InfluxDbException(response.statusText + '\n' + await response.text());
    }
}

export class Bucket {
    id: string;
    name: string;
}

export class Organization {
    id: string;
    name: string;
}

class Links {
    prev: string;
    self: string;
    next: string;
}

class OrganizationsResponse {
    links: Links;
    orgs: Organization[];
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

export function buildPredicate(measurement: string, tags: {[key: string]: string}): string {
    const conditions = measurement != null
        ? ['_measurement="' + escapeMeasurement(measurement) + '"']
        : [];
    for (const key in tags)
        conditions.push(escapeTag(key) + '="' + escapeTag(tags[key]) + '"');
    return conditions.join(' AND ');
}

export function buildTimedValueLine(
    measurement: string,
    tags: { [key: string] : string },
    value: number,
    timeStamp: number
): string {
    let line = escapeMeasurement(measurement);
    for (const key in tags)
        line += ',' + escapeTag(key) + '=' + escapeTag(tags[key]);
    line += ' value=' + value + ' ' + timeStamp;
    return line;
}

export class QueryBuilder {
    bucket: string;
    measurement: string;
    tags: { [key: string]: string };
    count: boolean;
    /** Unix timestamp measured in seconds */
    timeStart = 0;
    /** Unix timestamp measured in seconds */
    timeStop: number;

    setBucketName(bucket: string) {
        this.bucket = bucket;
        return this;
    }

    setMeasurementName(measurement: string) {
        this.measurement = measurement;
        return this;
    }

    setTags(tags: { [key: string]: string }) {
        this.tags = tags;
        return this;
    }

    setCount(count: boolean) {
        this.count = count;
        return this;
    }

    setRange(timeStart: number, timeStop: number) {
        this.timeStart = timeStart;
        this.timeStop = timeStop;
        return this;
    }

    private get rangeInnerQuery(): string {
        const timeStart = this.timeStart != null ? 'start: ' + this.timeStart : null;
        const timeStop = this.timeStop != null ? 'stop: ' + this.timeStop : null;
        const query = [timeStart, timeStop].filter(str => str != null).join(', ');
        return query;
    }

    build(): string {
        let filter = '';
        if (this.measurement)
            filter += `r._measurement == "${escapeMeasurement(this.measurement)}"`;
        if (this.tags)
            for (const tagKey in this.tags) {
                const tagValue = this.tags[tagKey];
                if (filter.length > 0)
                    filter += ` and `;
                filter += `r["${escapeTag(tagKey)}"] == "${escapeTag(tagValue)}"`;
            }
        let query = `from(bucket: "${this.bucket}")
            |> range(${this.rangeInnerQuery})
            |> filter(fn: (r) => ${filter})
            |> keep(columns: ["_time", "_value"])`;
        if (this.count)
            query += '\n |> count()'
        else
            query += '\n |> sort(columns: ["_time"])';
        query = query.split('\n').map(line => line.trim()).join('\n');
        return query;
    }
}