package dx.core.ir

import dx.api.DxExecutable

/**
  * Information used to link applets that call other applets. For example, a scatter
  * applet calls applets that implement tasks.
  * @param name executable name
  * @param inputs executable inputs
  * @param outputs exectuable outputs
  * @param dxExec API Object
  */
case class ExecutableLink(name: String,
                          inputs: Map[String, Type],
                          outputs: Map[String, Type],
                          dxExec: DxExecutable)
