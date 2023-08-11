<?php

/**
 * InfluxDB2.php
 *
 * -Description-
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 * @link       https://www.librenms.org
 *
 * @copyright  2023 Wesley Norris
 * @copyright  2020 Tony Murray
 * @copyright  2014 Neil Lathwood <https://github.com/laf/ http://www.lathwood.co.uk/fa>
 * @author     Wesley Norris <repnop@outlook.com>
 */

namespace LibreNMS\Data\Store;

use App\Polling\Measure\Measurement;
use InfluxDB2\Client;
use InfluxDB2\Model\WritePrecision;
use LibreNMS\Config;
use Log;

class InfluxDB2 extends BaseDatastore {
    /** @var \InfluxDB2\Client */
    private $client;
    /** @var string[] */
    private $allowed_measurements;
    /** @var string[] */
    private $allowed_fields;
    /** @var string[] */
    private $allowed_tags;

    public function __construct() {
        parent::__construct();

        $host = Config::get('influxdb2.host', 'localhost');
        $port = Config::get('influxdb2.port', 8086);
        $bucket = Config::get('influxdb2.bucket', 'librenms');
        $org = Config::get('influxdb2.org', 'librenms');
        $timeout = Config::get('influxdb2.timeout', 0);
        $token = Config::get('influxdb2.token', '');
        $verifySSL = Config::get('influxdb2.verifySSL', true);

        $this->client = new Client(['url' => "$host:$port", 'token' => $token, 'bucket' => $bucket, 'org' => $org, 'timeout' => $timeout, 'precision' => WritePrecision::NS, 'verifySSL' => $verifySSL]);
        $this->allowed_measurements = array_filter(array_map(function($s) { return trim($s); }, explode(',', Config::get('influxdb2.allowed_measurements', ''))), function($s) { return !empty($s); });
        $this->allowed_fields = array_filter(array_map(function($s) { return trim(strtoupper($s)); }, explode(',', Config::get('influxdb2.allowed_fields', ''))), function($s) { return !empty($s); });
        $this->allowed_tags = array_filter(array_map(function($s) { return trim(strtoupper($s)); }, explode(',', Config::get('influxdb2.allowed_tags', ''))), function($s) { return !empty($s); });
    }

    public function getName() {
        return 'InfluxDB2';
    }

    public static function isEnabled() {
        return Config::get('influxdb2.enable', false);
    }

    /**
     * Datastore-independent function which should be used for all polled metrics.
     *
     * RRD Tags:
     *   rrd_def     RrdDefinition
     *   rrd_name    array|string: the rrd filename, will be processed with rrd_name()
     *   rrd_oldname array|string: old rrd filename to rename, will be processed with rrd_name()
     *   rrd_step             int: rrd step, defaults to 300
     *
     * @param  array  $device
     * @param  string  $measurement  Name of this measurement
     * @param  array  $tags  tags for the data (or to control rrdtool)
     * @param  array|mixed  $fields  The data to update in an associative array, the order must be consistent with rrd_def,
     *                               single values are allowed and will be paired with $measurement
     */
    public function put($device, $measurement, $tags, $fields) {
        if (count($this->allowed_measurements) > 0 && array_search($measurement, $this->allowed_measurements) === false) {
            Log::debug('Skipping measurement which was not included in the list of allowed measurements', ['measurement' => $measurement]);
            return;
        }

        $stat = Measurement::start('write');

        $tmp_fields = [];
        $tmp_tags['hostname'] = $device['hostname'];
        foreach ($tags as $k => $v) {
            if (count($this->allowed_tags) > 0 && array_search(strtoupper($k), $this->allowed_tags) === false) {
                Log::debug('Skipping tag which was not included in the list of allowed tags', ['tag' => $k]);
                continue;
            }

            if (empty($v)) {
                $v = '_blank_';
            }
            $tmp_tags[$k] = $v;
        }
        foreach ($fields as $k => $v) {
            if (count($this->allowed_fields) > 0 && array_search(strtoupper($k), $this->allowed_fields) === false) {
                Log::debug('Skipping field which was not included in the list of allowed fields', ['field' => $k]);
                continue;
            }

            if ($k == 'time') {
                $k = 'rtime';
            }

            if (($value = $this->forceType($v)) !== null) {
                $tmp_fields[$k] = $value;
            }
        }

        if (empty($tmp_fields)) {
            Log::warning('All fields empty, skipping update', ['orig_fields' => $fields]);

            return;
        }

        Log::info('InfluxDB data: ', [
            'measurement' => $measurement,
            'tags' => $tags,
            'fields' => $fields,
        ]);

        try {
            $points = [
                new \InfluxDB2\Point(
                    $measurement,
                    $tmp_tags,
                    $tmp_fields // optional additional fields
                ),
            ];

            $this->client->createWriteApi()->write($points);
            $this->recordStatistic($stat->end());
        } catch (\Exception $e) {
            Log::error('InfluxDB2 exception: ' . $e->getMessage());
            Log::debug($e->getTraceAsString());
        }
    }

    private function forceType($data) {
        /*
         * It is not trivial to detect if something is a float or an integer, and
         * therefore may cause breakages on inserts.
         * Just setting every number to a float gets around this, but may introduce
         * inefficiencies.
         */

        if (is_numeric($data)) {
            return floatval($data);
        }

        return $data === 'U' ? null : $data;
    }

    /**
     * Checks if the datastore wants rrdtags to be sent when issuing put()
     *
     * @return bool
     */
    public function wantsRrdTags() {
        return false;
    }
}
