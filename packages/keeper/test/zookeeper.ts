import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import { GenericContainer } from "testcontainers"

import { KeeperConfig, makeKeeperConfig } from "../src/config"

export const makeKeeperTestConfig = M.gen(function* (_) {
  const container = yield* _(
    pipe(
      T.promise(() =>
        new GenericContainer("zookeeper:3.7.0").withExposedPorts(2181).start()
      ),
      M.make((c) => T.promise(() => c.stop()))
    )
  )

  return makeKeeperConfig({
    connectionString: `${container.getHost()}:${container.getMappedPort(2181)}`
  })
})

export const TestKeeperConfig = L.fromManaged(KeeperConfig)(makeKeeperTestConfig)
