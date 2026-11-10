package com.seshat.ingestion.filter;

import com.seshat.ingestion.dto.ApiError;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.UrlPathHelper;
import tools.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class BatchRequestSizeFilter extends OncePerRequestFilter {

    private final int maxRequestBytes;
    private final JsonMapper mapper;

    public BatchRequestSizeFilter(
            @Value("${seshat.ingestion.max-request-bytes:1048576}")
            int maxRequestBytes,
            JsonMapper mapper
    ) {
        if(maxRequestBytes <= 0 || maxRequestBytes == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("max-request-bytes must be between 1 and 2147483646.");
        }

        this.maxRequestBytes = maxRequestBytes;
        this.mapper = mapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = UrlPathHelper.defaultInstance
                .getPathWithinApplication(request);

        return !"POST".equalsIgnoreCase(request.getMethod())
                || !"/api/logs/batches".equals(path);
    }


    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {

        if(request.getContentLengthLong() > maxRequestBytes) {
            rejectTooLarge(response);
            return;
        }

        byte[] body = request.getInputStream().readNBytes(maxRequestBytes + 1);

        if(body.length > maxRequestBytes) {
            rejectTooLarge(response);
            return;
        }

        filterChain.doFilter(
                new CachedBodyRequest(request, body),
                response
        );
    }

    private void rejectTooLarge(HttpServletResponse response) throws IOException {
        byte[] body = mapper.writeValueAsBytes(
                new ApiError(
                        "REQUEST_TOO_LARGE",
                        "Request body exceed the configured byte limit."
                )
        );

        response.setStatus(HttpStatus.CONTENT_TOO_LARGE.value());
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setContentLength(body.length);
        response.getOutputStream().write(body);
    }

    private static class CachedBodyRequest extends HttpServletRequestWrapper {
        private final byte[] body;

        private CachedBodyRequest(HttpServletRequest request, byte[] body) {
            super(request);
            this.body = body;
        }

        @Override
        public int getContentLength() {
            return body.length;
        }

        @Override
        public long getContentLengthLong() {
            return body.length;
        }

        @Override
        public ServletInputStream getInputStream() {
            return new ServletInputStream() {
                private final ByteArrayInputStream input = new ByteArrayInputStream(body);

                @Override
                public int read() {
                    return input.read();
                }

                @Override
                public int read(byte[] bytes, int offset, int length) {
                    return input.read(bytes, offset, length);
                }

                @Override
                public boolean isFinished() {
                    return input.available() == 0;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(ReadListener listener) {
                    throw new IllegalStateException("Only blocking reads are supported.");
                }
            };
        }
        @Override
        public BufferedReader getReader() throws IOException {
            String encoding = getCharacterEncoding();

            Charset charset = encoding == null
                    ? StandardCharsets.UTF_8
                    : Charset.forName(encoding);

            return new BufferedReader(new InputStreamReader(getInputStream(), charset));
        }
    }

}
