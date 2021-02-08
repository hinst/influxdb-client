import fetch from 'node-fetch';
const http = require('http');
const https = require('https');
const httpAgent = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });
const agent = (_parsedURL: URL) => _parsedURL.protocol == 'https:' ? httpsAgent : httpAgent;

export class InfluxDb {
    apiToken: string;
    apiUrl: string;

    get headers() {
        return {
            Authorization: 'Token ' + this.apiToken
        };
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
        if (!response.ok)
            throw new Error(response.statusText + '\n' + await response.text());
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
