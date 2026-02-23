<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Symfony\AI\Mate\Transport;

use Mcp\Schema\JsonRpc\Error;
use Mcp\Server\Transport\BaseTransport;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\Http\HttpServer;
use React\Http\Message\Response as ReactResponse;
use React\Socket\SocketServer;
use Symfony\Component\Uid\Uuid;

/**
 * Streamable HTTP transport for the MCP server using ReactPHP.
 *
 * Implements the MCP Streamable HTTP transport spec (2025-03-26):
 * - POST /mcp  → process JSON-RPC message, respond with application/json
 * - DELETE /mcp → terminate the session
 * - OPTIONS /mcp → CORS preflight
 *
 * Each POST is handled synchronously within ReactPHP's event loop.
 * Parallel tool calls from the client are queued and processed in order.
 *
 * @extends BaseTransport<mixed>
 *
 * @author Guillaume Morel
 */
final class ReactPhpHttpServerTransport extends BaseTransport
{
    private ?string $immediateResponse = null;
    private int $immediateStatusCode = 200;

    public function __construct(
        private readonly string $host = '0.0.0.0',
        private readonly int $port = 3000,
        ?LoggerInterface $logger = null,
    ) {
        parent::__construct($logger);
    }

    public function send(string $data, array $context): void
    {
        $this->immediateResponse = $data;
        $this->immediateStatusCode = $context['status_code'] ?? 200;

        if (isset($context['session_id'])) {
            $this->sessionId = $context['session_id'];
        }
    }

    public function listen(): mixed
    {
        $http = new HttpServer(function (ServerRequestInterface $request): ReactResponse {
            return $this->handleHttpRequest($request);
        });

        $socket = new SocketServer(\sprintf('%s:%d', $this->host, $this->port));
        $http->listen($socket);

        $this->logger->info(\sprintf('MCP HTTP server listening on http://%s:%d/mcp', $this->host, $this->port));

        Loop::run();

        return null;
    }

    private function handleHttpRequest(ServerRequestInterface $request): ReactResponse
    {
        $path = $request->getUri()->getPath();
        $method = $request->getMethod();

        if ('OPTIONS' === $method) {
            return new ReactResponse(204, $this->corsHeaders());
        }

        if ('DELETE' === $method && '/mcp' === $path) {
            $sessionIdString = $request->getHeaderLine('Mcp-Session-Id');
            if ('' !== $sessionIdString) {
                $this->handleSessionEnd(Uuid::fromString($sessionIdString));
            }

            return new ReactResponse(200, $this->corsHeaders());
        }

        if ('POST' !== $method || '/mcp' !== $path) {
            return new ReactResponse(
                404,
                array_merge($this->corsHeaders(), ['Content-Type' => 'text/plain']),
                'Not Found',
            );
        }

        return $this->handlePostRequest($request);
    }

    private function handlePostRequest(ServerRequestInterface $request): ReactResponse
    {
        $sessionIdString = $request->getHeaderLine('Mcp-Session-Id');
        $sessionId = '' !== $sessionIdString ? Uuid::fromString($sessionIdString) : null;

        $this->immediateResponse = null;
        $this->immediateStatusCode = 200;
        $this->sessionId = $sessionId;
        $this->sessionFiber = null;

        $body = (string) $request->getBody();
        $this->handleMessage($body, $sessionId);

        if (null !== $this->immediateResponse) { // @phpstan-ignore-line notIdentical.alwaysFalse
            return new ReactResponse(
                $this->immediateStatusCode,
                array_merge($this->corsHeaders(), ['Content-Type' => 'application/json']),
                $this->immediateResponse,
            );
        }

        if (null !== $this->sessionFiber) {
            $this->runFiberToCompletion();
        }

        $resolvedSessionId = $this->sessionId;
        $messages = $this->getOutgoingMessages($resolvedSessionId);

        $headers = array_merge($this->corsHeaders(), ['Content-Type' => 'application/json']);

        if (null !== $resolvedSessionId) {
            $headers['Mcp-Session-Id'] = $resolvedSessionId->toRfc4122();
        }

        if (empty($messages)) {
            return new ReactResponse(202, $headers);
        }

        $bodies = array_column($messages, 'message');
        $responseBody = 1 === \count($bodies) ? $bodies[0] : '['.implode(',', $bodies).']';

        return new ReactResponse(200, $headers, $responseBody);
    }

    private function runFiberToCompletion(): void
    {
        if (null === $this->sessionFiber) {
            return;
        }

        while ($this->sessionFiber->isSuspended()) {
            $pendingRequests = $this->getPendingRequests($this->sessionId);

            if (empty($pendingRequests)) {
                $yielded = $this->sessionFiber->resume();
                $this->handleFiberYield($yielded, $this->sessionId);
                continue;
            }

            $resumed = false;
            foreach ($pendingRequests as $pending) {
                $requestId = (int) $pending['request_id'];
                $timestamp = (int) $pending['timestamp'];
                $timeout = (int) ($pending['timeout'] ?? 120);

                $response = $this->checkForResponse($requestId, $this->sessionId);

                if (null !== $response) {
                    $yielded = $this->sessionFiber->resume($response);
                    $this->handleFiberYield($yielded, $this->sessionId);
                    $resumed = true;
                    break;
                }

                if (time() - $timestamp >= $timeout) {
                    $error = Error::forInternalError('Request timed out', $requestId);
                    $yielded = $this->sessionFiber->resume($error);
                    $this->handleFiberYield($yielded, $this->sessionId);
                    $resumed = true;
                    break;
                }
            }

            if (!$resumed) {
                usleep(10_000);
            }
        }

        $this->sessionFiber = null;
    }

    /**
     * @return array<string, string>
     */
    private function corsHeaders(): array
    {
        return [
            'Access-Control-Allow-Origin' => '*',
            'Access-Control-Allow-Methods' => 'POST, DELETE, OPTIONS',
            'Access-Control-Allow-Headers' => 'Content-Type, Mcp-Session-Id, Mcp-Protocol-Version, Last-Event-ID, Authorization, Accept',
            'Access-Control-Expose-Headers' => 'Mcp-Session-Id',
        ];
    }
}
