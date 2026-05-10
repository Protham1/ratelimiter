import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    // Step 1: Ramp up to 50 virtual users over 10 seconds
    { duration: '10s', target: 50 },
    // Step 2: Hold steady at 50 virtual users for 20 seconds (triggering Sliding Window / Token Bucket)
    { duration: '20s', target: 50 },
    // Step 3: Massive burst to 300 virtual users (Triggering Exponential Backoff)
    { duration: '10s', target: 300 },
    // Step 4: Hold massive spam for 10 seconds
    { duration: '10s', target: 300 },
    // Step 5: Ramp down
    { duration: '10s', target: 0 },
  ],
  thresholds: {
    // We expect 95% of requests to be completed in under 50ms, because our Redis logic is fast!
    http_req_duration: ['p(95)<50'], 
  },
};

export default function () {
  const url = 'http://host.docker.internal:8000/check';
  const payload = JSON.stringify({
    key: 'k6_test_user'
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);

  // K6 validation checks
  check(res, {
    'is status 200': (r) => r.status === 200,
    'has JSON response': (r) => r.headers['Content-Type'].includes('application/json'),
  });

  // Short sleep to simulate real user pacing
  sleep(0.1);
}
