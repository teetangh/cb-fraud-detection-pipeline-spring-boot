package com.fraud.gateway;

import com.fraud.gateway.infra.HmacJwtVerifier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure unit test, no containers.
 *
 * <p>Auth is the one place where a test that only checks the happy path is
 * actively harmful: "a valid token is accepted" passes just as well on a
 * verifier that accepts everything. Every case below is a rejection.
 */
@DisplayName("HMAC JWT verification")
class HmacJwtVerifierTest {

    private static final String SECRET = "test-secret-value-for-unit-tests";
    private final HmacJwtVerifier verifier = new HmacJwtVerifier(SECRET);

    @Test
    @DisplayName("accepts a token signed with the shared secret")
    void acceptsValidToken() {
        assertThat(verifier.verify("Bearer " + mint(SECRET))).isTrue();
    }

    @Test
    @DisplayName("accepts a bare token without the Bearer prefix")
    void acceptsBareToken() {
        assertThat(verifier.verify(mint(SECRET))).isTrue();
    }

    @Test
    @DisplayName("REJECTS a token signed with a different secret")
    void rejectsWrongSecret() {
        // The core property. If this passes, the shared secret means nothing.
        assertThat(verifier.verify("Bearer " + mint("a-completely-different-secret"))).isFalse();
    }

    @Test
    @DisplayName("REJECTS a token whose payload was tampered with after signing")
    void rejectsTamperedPayload() {
        String token = mint(SECRET);
        String[] parts = token.split("\\.");
        String forgedPayload = Base64.getUrlEncoder().withoutPadding().encodeToString(
                "{\"sub\":\"attacker\",\"iat\":0,\"exp\":9999999999}".getBytes(StandardCharsets.UTF_8));

        // Signature kept, payload swapped — the classic forgery attempt.
        assertThat(verifier.verify(parts[0] + "." + forgedPayload + "." + parts[2])).isFalse();
    }

    @Test
    @DisplayName("REJECTS structurally invalid input without throwing")
    void rejectsMalformed() {
        assertThat(verifier.verify(null)).isFalse();
        assertThat(verifier.verify("")).isFalse();
        assertThat(verifier.verify("Bearer ")).isFalse();
        assertThat(verifier.verify("Bearer not-a-jwt")).isFalse();
        assertThat(verifier.verify("Bearer only.two")).isFalse();
        assertThat(verifier.verify("Bearer a.b.c.d")).isFalse();
        // A malformed token must be a clean false, never a 500 — otherwise
        // garbage input becomes a denial-of-service surface.
        assertThat(verifier.verify("Bearer !!!.@@@.###")).isFalse();
    }

    @Test
    @DisplayName("REJECTS the alg=none unsigned-token attack")
    void rejectsEmptySignature() {
        String token = mint(SECRET);
        String[] parts = token.split("\\.");
        assertThat(verifier.verify(parts[0] + "." + parts[1] + ".")).isFalse();
    }

    private static String mint(String secret) {
        try {
            var b64 = Base64.getUrlEncoder().withoutPadding();
            String header = b64.encodeToString("{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
            long now = Instant.now().getEpochSecond();
            String payload = b64.encodeToString(
                    ("{\"sub\":\"mock-payment-api\",\"iat\":" + now + ",\"exp\":" + (now + 300) + "}")
                            .getBytes(StandardCharsets.UTF_8));
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            String signingInput = header + "." + payload;
            return signingInput + "." + b64.encodeToString(
                    mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
